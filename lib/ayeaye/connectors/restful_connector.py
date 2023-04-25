"""
Created on 14 Jul 2022

@author: si
"""
import json
from time import time

try:
    import requests
except ModuleNotFoundError:
    pass


from ayeaye.connectors.base import DataConnector, AccessMode
from ayeaye.pinnate import Pinnate


class RestfulConnector(DataConnector):
    engine_type = ["http://", "https://"]
    optional_args = {
        "headers": None,
        "keep_alive": True,  # http 1.1 mode is the default
    }

    class RawData:
        "Wrapper class to indicate data shouldn't be cast to JSON"

        def __init__(self, data):
            """
            @param data: (mixed) something that can be passed to :requests:
            """
            self.data = data

    def __init__(self, *args, **kwargs):
        """
        Connector to read and write to JSON RestFul APIs.

        See-
        * https://en.wikipedia.org/wiki/Representational_state_transfer
        * https://jsonapi.org/

        For args: @see :class:`connectors.base.DataConnector`

        additional args for RestfulConnector
            headers (dictionary) - key value pairs to pass in the http header. These are often used
                                for authentication keys.
            keep_alive (Boolean) - use sessions to persist cookies and reuse TCP sockets. This mode
                                also enables retry on fail to connect.

        Connection information-
            engine_url format is
            http[s]://<base_url>
            e.g. https://twitter.com
            all requests made by this connector are expected to start with the http[s]://base_url
            self.engine_url in the code is this URI
        """
        super().__init__(*args, **kwargs)

        # config flags - these could become optional args later. Hard coded for now
        # --------------------------
        # Request is retired for these HTTP status codes
        self.retry_on_status = [
            500,
            502,
            503,
            504,
        ]
        self.raise_exception_on_404 = False
        self.raise_exception_on_400s = True
        self.raise_exception_on_500 = True
        self.max_retries = 3
        # --------------------------

        # updated whilst running
        self.last_http_status = None
        self.last_headers = None
        self._requests = None  # initiated by :method:`connect`
        self.statistics = None  # OK to read this
        self.reset_stats()
        self.profiler = ProfileRequest(statistics=self.statistics)

    @classmethod
    def as_raw(cls, data):
        "Wrapper to mark data a not needing to be cast into JSON"
        return cls.RawData(data=data)

    def reset_stats(self):
        """clear profiling requests made during lifetime of object
        - 'requests_total_time' is time spent waiting for web server (sec)
        """

        self.statistics = Pinnate(
            {
                "requests_count": 0,
                "requests_total_time": 0.0,
                "requests_slowest_time": None,
                "requests_slowest_url": None,
            }
        )

    def close_connection(self):
        super().close_connection()
        if self._requests is not None:
            self._requests = None
        self.reset_stats()

    def connect(self):
        super().connect()
        if self._requests is None:
            if self.keep_alive:
                self._requests = requests.Session()

                retry_config = requests.adapters.Retry(
                    total=self.max_retries,
                    backoff_factor=0.1,
                    status_forcelist=self.retry_on_status,
                )

                self._requests.mount(
                    "http://", requests.adapters.HTTPAdapter(max_retries=retry_config)
                )
                self._requests.mount(
                    "https://", requests.adapters.HTTPAdapter(max_retries=retry_config)
                )
            else:
                self._requests = requests

    def __len__(self):
        """
        Doesn't make sense for a RestFul API
        """
        raise NotImplementedError("Not available for API connector")

    def __getitem__(self, key):
        """
        Doesn't make sense for a RestFul API
        """
        raise NotImplementedError("Not available for API connector")

    def __iter__(self):
        """
        Doesn't make sense for a RestFul API
        """
        raise NotImplementedError("Not available for API connector")

    @property
    def schema(self):
        raise NotImplementedError("Not sure if possible for the API connector")

    @property
    def not_found(self):
        """
        @return: bool
            Return True if the last API request was for an entity that didn't exist.
        """
        return self.last_http_status == 404

    @property
    def response_headers(self):
        """
        @return: list of str
            http headers from server for last request.
        """
        return self.last_headers

    def qualify_url(self, url):
        """
        Ensure URL is fully formed so is ready to pass to requests library.

        @param url: str
            either url without self.engine_url or already a fully qualified url

        @return: str
            fully qualified url.
        """
        if url.startswith("http://") or url.startswith("https://"):
            msg = (
                "RestfulConnector needs all requests to start with the same URL base. Current"
                f" set to {self.engine_url}, this url is {url}/"
            )
            assert url.startswith(self.engine_url), msg
            return url
        else:
            return self.engine_url + url

    def get(self, url, params=None):
        """
        HTTP GET

        @param url: str
            either fully qualified or url suffix (e.g. 'site/' that needs self.engine_url)

        @param params: dictionary or :class:`Pinnate` of arguments to append to the URL.
                        e.g.
                        self.engine_url="https://api.mystuff.com/abc
                        c.get("/xyz", {'q':'search_term'})
                         becomes
                        https://api.mystuff.com/abc/xyz?q=search_term

        @return: :class:`Pinnate` object representing the JSON response
        """
        if self.access not in (AccessMode.READ, AccessMode.READWRITE):
            raise ValueError("Read attempted on dataset not opened in READ mode.")

        self.connect()

        if isinstance(params, Pinnate):
            _params = params.as_dict()
        else:
            _params = params

        url_ = self.qualify_url(url)
        with self.profiler(url_, _params):
            try:
                r = self._requests.get(url_, params=_params, headers=self.headers)
            except requests.ConnectionError as c:
                msg = f"Failed to GET from {url_}"
                raise RestfulConnectorConnectionError(msg, details=str(c))

        self._post_request_checks(r)
        serialised_request = Pinnate(r.json())
        return serialised_request

    def post(self, url, data):
        """
        HTTP POST

        @param url: str
            either fully qualified or url suffix (e.g. 'site/' that needs self.engine_url)

        @param data: :class:`Pinnate` object or something that serialises to JSON
         OR
        :class:`RestfulConnector.RawData`

        @return: :class:`Pinnate` object representing the JSON response or None if response
            doesn't contain valid JSON.
        """
        if self.access not in (AccessMode.WRITE, AccessMode.READWRITE):
            raise ValueError("Write attempted on dataset not opened in write mode.")

        self.connect()

        url_ = self.qualify_url(url)
        headers = {"Content-type": "application/json"}

        if isinstance(self.headers, dict):
            headers.update(self.headers)

        if isinstance(data, RestfulConnector.RawData):
            request_data = data.data
        elif isinstance(data, Pinnate):
            request_data = json.dumps(data.as_dict())
        else:
            request_data = json.dumps(data)

        with self.profiler(url_):
            try:
                r = self._requests.post(url_, data=request_data, headers=headers)
            except requests.ConnectionError as c:
                msg = f"Failed to POST to {url_}"
                raise RestfulConnectorConnectionError(msg, details=c.message)

        self._post_request_checks(r)

        # reply doc. is optional
        try:
            reply_doc = r.json()
        except requests.exceptions.JSONDecodeError:
            # r.text could contain something but probably empty
            return None

        serialised_request = Pinnate(reply_doc)
        return serialised_request

    def patch(self, url, data):
        """
        HTTP PATCH

        @param data: Pinnate object or something that serialises to JSON
        """
        if self.access not in (AccessMode.WRITE, AccessMode.READWRITE):
            raise ValueError("Write attempted on dataset not opened in write mode.")

        self.connect()

        url_ = self.qualify_url(url)
        headers = {"Content-type": "application/json"}

        if isinstance(self.headers, dict):
            headers.update(self.headers)

        if isinstance(data, Pinnate):
            json_data = json.dumps(data.as_dict())
        else:
            json_data = json.dumps(data)

        if isinstance(data, Pinnate):
            json_data = json.dumps(data.as_dict())
        else:
            json_data = json.dumps(data)

        with self.profiler(url_):
            try:
                r = self._requests.patch(url_, data=json_data, headers=headers)
            except requests.ConnectionError as c:
                msg = f"Failed to PATCH {url_}"
                raise RestfulConnectorConnectionError(msg, details=c.message)

        self._post_request_checks(r)

        # reply doc. is optional
        try:
            reply_doc = r.json()
        except requests.exceptions.JSONDecodeError:
            # r.text could contain something but probably empty
            return None

        serialised_request = Pinnate(reply_doc)
        return serialised_request

    def _post_request_checks(self, r):
        """
        Update flags etc. after each request
        """
        self.last_http_status = r.status_code
        self.last_headers = r.headers
        msg = f"Received http status: {r.status_code}"

        if self.last_http_status >= 400:
            try:
                as_json = r.json()
                detailed_message = as_json["message"]
            except:
                detailed_message = None

        if r.status_code == 500 and self.raise_exception_on_500:
            raise RestfulConnectorConnectionError(
                msg, details=detailed_message, last_http_code=r.status_code
            )

        if r.status_code == 404 and self.raise_exception_on_404:
            raise RestfulConnectorConnectionError(
                msg, details=detailed_message, last_http_code=r.status_code
            )

        if (
            r.status_code != 404
            and r.status_code >= 400
            and r.status_code < 500
            and self.raise_exception_on_400s
        ):
            raise RestfulConnectorConnectionError(
                msg, details=detailed_message, last_http_code=r.status_code
            )


class ProfileRequest:
    """
    Track time taken for API requests.

    Tied into RestfulConnector through statistics so is really a private class
    """

    def __init__(self, statistics):
        """
        @param statistics: Pinnate with attribs -  requests_count,
                requests_total_time, requests_slowest_time,
                requests_slowest_url
        """
        self.statistics = statistics
        self.url = None

    def __call__(self, url, params=None):
        """
        @param url: (str)
            used in statistics
        """
        self.url = url

        # TODO just dumping params to string. This isn't a correct URL encoded url
        if params:
            self.url += str(params)

        return self

    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, type_, value, traceback):
        elapsed = time() - self.start
        self.statistics.requests_count += 1
        self.statistics.requests_total_time += elapsed

        if (
            self.statistics.requests_slowest_time == None
            or elapsed > self.statistics.requests_slowest_time
        ):
            self.statistics.requests_slowest_time = elapsed
            self.statistics.requests_slowest_url = self.url


class RestfulConnectorConnectionError(Exception):
    def __init__(self, message, details=None, last_http_code=None):
        super().__init__(message)
        self.message = message
        self.details = details
        self.last_http_code = last_http_code

    def __str__(self):
        if self.details:
            return "%s [%s]" % (self.message, self.details)
        return self.message
