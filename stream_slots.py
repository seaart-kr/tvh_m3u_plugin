"""Bound concurrent relays across the FF worker processes."""
import errno
import fcntl
import os


def acquire_slot(directory, limit):
    os.makedirs(directory, mode=0o700, exist_ok=True)
    for index in range(limit):
        fd = os.open(os.path.join(directory, str(index)),
                     os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except OSError as exc:
            os.close(fd)
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
    return None
