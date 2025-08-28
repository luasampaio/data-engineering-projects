def add_hash(input: dict):
    key_str = []
    for k, v in input.items():
        if isinstance(v, str):
            key_str.append(v)

    return str(hashlib.md5(str(''.join(key_str)).encode()).hexdigest())

