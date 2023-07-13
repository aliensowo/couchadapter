import base64

import requests
import urllib3
from django.conf import settings


class PreviewMethods:
    """
    ...
    """

    ip = settings.COUCHBASE_HOSTS + ":8091"
    username = settings.COUCHBASE_USER
    password = settings.COUCHBASE_PASSWORD
    timeout = settings.API_TO if settings.API_TO else 120
    bucket = None
    headers = {
        "Authorization": "Basic " + base64.encodebytes(("%s:%s" % (username, password)).encode()).decode().strip(),
        "Content-Type": "application/x-www-form-urlencoded",
        "Connection": "keep-alive",
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/14.0.2 Safari/605.1.15",
    }

    def __init__(self, bucket):
        self.bucket = bucket

    def make_scope(
        self, scope  # type: str
    ) -> bool:
        """

        :param scope:
        :return: ex:{"uid":18}
        """
        for node in self.ip[:-5].split(","):
            try:
                self.headers["Content-Length"] = str(len(scope) + 7)
                r = requests.post(
                    url=f"http://{node}:8091/pools/default/buckets/{self.bucket}/collections",
                    headers=self.headers,
                    data={"name": scope},
                    timeout=self.timeout,
                )
                if r.status_code == 200:
                    return True
            except urllib3.exceptions.MaxRetryError:
                pass

    def make_coll(
        self,
        scope,  # type: str
        collection_in_scope,  # type: str
    ) -> bool:
        """
        Response example:
            {"uid":20}

        Response codes:
            HTTP/1.1 200 OK
            HTTP/1.1 401 Unauthorized
            HTTP/1.1 404 Object Not Found

        :param scope:
        :param collection_in_scope:
        :return: {"uid":18}
        """
        for node in self.ip[:-5].split(","):
            try:
                self.headers["Content-Length"] = str(len(collection_in_scope) + 7)
                r = requests.post(
                    url=f"http://{node}:8091/pools/default/buckets/{self.bucket}/collections/{scope}",
                    headers=self.headers,
                    data={"name": collection_in_scope},
                    timeout=self.timeout,
                )
                if r.status_code == 200:
                    return True

            except urllib3.exceptions.MaxRetryError:
                pass

    def del_scope(
        self, scope  # type: str
    ) -> bool:
        """
        Scopes can be dropped, by means of the REST API.

        Scopes are dropped by means of the DELETE

        Response example:
            {"uid":20}

        Response codes:
            HTTP/1.1 200 OK
            HTTP/1.1 401 Unauthorized
            HTTP/1.1 404 Object Not Found

        :param scope: scope_name: str
        :return: bool
        """
        r = requests.delete(
            url=f"http://{self.ip}/pools/default/buckets/{self.bucket}/collections/{scope}",
            headers=self.headers,
        )
        return True if r.status_code == 200 else False

    def del_coll(
        self,
        scope,  # type: str
        collection_in_scope,  # type: str
    ) -> bool:
        """
        Scopes can be dropped, by means of the REST API.

        Scopes are dropped by means of the DELETE

        Response example:
            {"uid":20}

        Response codes:
            HTTP/1.1 200 OK
            HTTP/1.1 401 Unauthorized
            HTTP/1.1 404 Object Not Found

        :param collection_in_scope: str
        :param scope: scope_name: str
        :return: bool
        """
        r = requests.delete(
            url=f"http://{self.ip}/pools/default/buckets/{self.bucket}/collections/{scope}/{collection_in_scope}",
            headers=self.headers,
        )
        return True if r.status_code == 200 else False
