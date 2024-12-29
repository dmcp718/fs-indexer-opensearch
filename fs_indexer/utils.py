import xxhash

def calculate_checksum(file_path):
    """Calculate XXH64 checksum of a file."""
    try:
        with open(file_path, 'rb') as f:
            xxh64 = xxhash.xxh64()
            for chunk in iter(lambda: f.read(8192), b''):
                xxh64.update(chunk)
            return xxh64.hexdigest()
    except Exception as e:
        print(f"Error calculating checksum for {file_path}: {e}")
        return None
