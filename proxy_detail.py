

def proxy_info():
    username = "geonode_29EgtblBUB-type-residential"
    password = "daadd8a2-2815-4c8e-b59e-5f2ff2f6eace"
    GEONODE_DNS = "proxy.geonode.io:9000"
    # proxy = {"http": "http://{}:{}@{}".format(username, password, GEONODE_DNS)}
    proxy = f"http://{username}:{password}@{GEONODE_DNS}"
    return proxy


