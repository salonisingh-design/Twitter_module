

def proxy_info():
    username = "geonode_29EgtblBUB-type-residential"
    password = "daadd8a2-2815-4c8e-b59e-5f2ff2f6eace"
    GEONODE_DNS = "proxy.geonode.io:9000"
    # proxy = {"http": "http://{}:{}@{}".format(username, password, GEONODE_DNS)}
    proxy = f"http://{username}:{password}@{GEONODE_DNS}"
    return proxy


def cookie_proxy_info():
    username = "geonode_29EgtblBUB-type-residential"
    password = "daadd8a2-2815-4c8e-b59e-5f2ff2f6eace"
    GEONODE_DNS = "proxy.geonode.io:9000"

    # Build proxy URL
    proxy_url = f"http://{username}:{password}@{GEONODE_DNS}"

    proxies = {
        "http": proxy_url,
        "https": proxy_url
    }
    # Return as dictionary for requests
    return proxies

# Usage

