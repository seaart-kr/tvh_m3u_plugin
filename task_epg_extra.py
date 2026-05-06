# -*- coding: utf-8 -*-
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from html.parser import HTMLParser

import requests


DEFAULT_DLIVE_SCHEDULE_URL = 'http://ch1.dlive.kr/?act=info.page&pcode=schedule&dt={date}'
DEFAULT_DLIVE_SOURCE_NAME = '딜라이브TV'


class _DLiveScheduleParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_td = False
        self.current_cell = []
        self.current_row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'tr':
            self.current_row = []
        elif tag.lower() == 'td':
            self.in_td = True
            self.current_cell = []

    def handle_data(self, data):
        if self.in_td:
            self.current_cell.append(data)

    def handle_endtag(self, tag):
        lower = tag.lower()
        if lower == 'td':
            self.in_td = False
            self.current_row.append(''.join(self.current_cell).strip())
        elif lower == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)
            self.current_row = []


def _normalize_time_text(value):
    text = re.sub(r'\s+', '', str(value or ''))
    return text


def _extract_programs_from_html(html_text, target_date):
    parser = _DLiveScheduleParser()
    parser.feed(html_text or '')

    programs = []
    for cols in parser.rows:
        if len(cols) < 2:
            continue
        time_text = _normalize_time_text(cols[0])
        if not re.match(r'^\d{1,2}:\d{2}$', time_text):
            continue
        title = ' '.join([str(x or '').strip() for x in cols[1:] if str(x or '').strip()])
        if not title:
            continue
        programs.append({
            'time': f'{target_date} {time_text}',
            'title': title,
        })
    return programs


def fetch_dlive_schedule(url_template, target_date, timeout=15):
    url = str(url_template or DEFAULT_DLIVE_SCHEDULE_URL).strip() or DEFAULT_DLIVE_SCHEDULE_URL
    url = url.replace('{date}', target_date)
    resp = requests.get(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            )
        },
        timeout=timeout,
    )
    resp.raise_for_status()
    resp.encoding = 'utf-8'
    return _extract_programs_from_html(resp.text, target_date)


def build_dlive_epg_xml_bytes(channel_name, channel_id, url_template='', days=2):
    channel_name = str(channel_name or '').strip() or DEFAULT_DLIVE_SOURCE_NAME
    channel_id = str(channel_id or '').strip() or 'DLIVE_SONGPA'
    url_template = str(url_template or '').strip() or DEFAULT_DLIVE_SCHEDULE_URL
    days = max(1, int(days or 2))

    dates = [
        (datetime.now() + timedelta(days=offset)).strftime('%Y-%m-%d')
        for offset in range(days)
    ]

    all_programs = []
    for target_date in dates:
        all_programs.extend(fetch_dlive_schedule(url_template, target_date))

    root = ET.Element('tv')
    channel = ET.SubElement(root, 'channel', id=channel_id)
    ET.SubElement(channel, 'display-name').text = channel_name
    if channel_name != DEFAULT_DLIVE_SOURCE_NAME:
        ET.SubElement(channel, 'display-name').text = DEFAULT_DLIVE_SOURCE_NAME

    for index, item in enumerate(all_programs):
        try:
            start_dt = datetime.strptime(item['time'], '%Y-%m-%d %H:%M')
        except Exception:
            continue

        if index + 1 < len(all_programs):
            try:
                end_dt = datetime.strptime(all_programs[index + 1]['time'], '%Y-%m-%d %H:%M')
            except Exception:
                end_dt = start_dt + timedelta(hours=1)
        else:
            end_dt = start_dt + timedelta(hours=1)

        programme = ET.SubElement(
            root,
            'programme',
            start=start_dt.strftime('%Y%m%d%H%M%S +0900'),
            stop=end_dt.strftime('%Y%m%d%H%M%S +0900'),
            channel=channel_id,
        )
        ET.SubElement(programme, 'title', lang='ko').text = str(item.get('title') or '').strip()

    return ET.tostring(root, encoding='utf-8', xml_declaration=True)


def merge_xmltv_files(base_xml_path, extra_xml_items, output_path):
    tree = ET.parse(base_xml_path)
    root = tree.getroot()

    existing_channel_ids = set()
    for child in list(root):
        if str(child.tag).split('}', 1)[-1] == 'channel':
            channel_id = str(child.attrib.get('id') or '').strip()
            if channel_id:
                existing_channel_ids.add(channel_id)

    for item in extra_xml_items:
        xml_bytes = item.get('xml_bytes') if isinstance(item, dict) else None
        if not xml_bytes:
            continue
        extra_root = ET.fromstring(xml_bytes)
        for child in list(extra_root):
            tag = str(child.tag).split('}', 1)[-1]
            if tag == 'channel':
                channel_id = str(child.attrib.get('id') or '').strip()
                if channel_id and channel_id in existing_channel_ids:
                    continue
                if channel_id:
                    existing_channel_ids.add(channel_id)
            root.append(child)

    tree.write(output_path, encoding='utf-8', xml_declaration=True)
