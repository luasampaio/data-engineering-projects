def hello(name, capitalize=False, repetitions=1):
    """Says hello to you."""
    if capitalize:
        name = name.upper()

    for _ in range(repetitions):
        print(f"Hello {name}")



hello("luciana", capitalize=True, repetitions=3)

