def parse_cookies(file_path):
    """
    Reads a Netscape cookie file and returns a string suitable for the Cookie header.

    The cookie file should be in the format:
      domain, flag, path, secure, expiration, name, value

    If the cookie name is empty, only the value is output.
    """
    cookies = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines or comments
            if not line or line.startswith("#"):
                continue
            # Split the line into its components (fields are tab-separated)
            parts = line.split("\t")
            # Ensure we have at least 7 fields
            if len(parts) < 7:
                continue
            name = parts[5].strip()
            value = parts[6].strip()
            # If the name is empty, output only the value
            cookie_str = f"{name}={value}" if name else value
            cookies.append(cookie_str)

    # Join the cookies with "; " and prepend "Cookie: "
    return "Cookie: " + "; ".join(cookies)


if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print("Usage: python cookie_parser.py <path_to_cookie_file>")
        sys.exit(1)
    file_path = sys.argv[1]
    result = parse_cookies(file_path)
    print(result)
