def load_properties(filepath='../../properties/connection.properties', sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath) as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip().strip('"')
                props[key] = value
    return props


properties = load_properties()


def get_auth_type():
    return properties.get("AUTH_TYPE")


def get_auth_provider_type():
    return properties.get("AUTH_PROVIDER_TYPE")


def get_auth_client_id():
    return properties.get("AUTH_CLIENT_ID")


def get_auth_client_secret():
    return properties.get("AUTH_CLIENT_SECRET")


def get_auth_client_endpoint():
    return properties.get("AUTH_CLIENT_ENDPOINT")


def get_path_hotels():
    return properties.get("PATH_HOTELS")


def get_path_weather():
    return properties.get("PATH_WEATHER")


def get_opencage_api_key():
    return properties.get("OPENCAGE_API_KEY")
