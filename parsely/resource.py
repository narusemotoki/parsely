import os.path


resource_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources')


def resource_filename(filename: str) -> str:
    return os.path.join(resource_dir, filename)
