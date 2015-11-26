#
# A thin wrapper around the SimpleDB api using aiohttp.
# 
# Only support us-east-1 region because lazy.
# Requires Python >=3.5
#

# Imports
import aiohttp
import asyncio
import base64
import botocore
from botocore import session
from collections import namedtuple
import datetime
from enum import Enum
import hashlib
import hmac
import json
import logging
import os
import requests
import sys
import urllib
import xmltodict


# Credentials container
AWSCreds = namedtuple('AWSCreds', ['access_key', 'secret_key'])

# Signed query string container
SignedQueryString = namedtuple('SignedQueryString', [
    'query_string',
    'signature'
])

# Response container
Response = namedtuple('Response', [
    'body_text',
    'body_dict',
    'status_code',
])

# Top-level Logger
LOGGER_BASENAME = 'aiosdb'
LOG = logging.getLogger(LOGGER_BASENAME)

# Constants
API_SCHEME='https'
API_HOST = 'sdb.amazonaws.com'
API_ENDPOINT='{scheme}://{host}'.format(scheme=API_SCHEME, host=API_HOST)
API_PATH = '/'
API_URL='{endpoint}{path}'.format(endpoint=API_ENDPOINT, path=API_PATH,)
API_VERSION = '2009-04-15'
API_REQUEST_CONTENT_TYPE = 'application/x-www-form-urlencoded; charset=utf-8'
API_REQUEST_METHOD = 'POST'


#### Classes ################################################################


class Action(Enum):
    """
    An enumeration of SimpleDB API actions
    This enum is notably incomplete.  Other actions will likely work without
    any changes to this module, but these are the ones that have been tested
    and are known to work.
    """
    list_domains = 'ListDomains'
    batch_delete_attributes = 'BatchDeleteAttributes'
    batch_put_attributes = 'BatchPutAttributes'


class AWSv2Signer(object):
    """
    Class for signing SimpleDB API requests.
    """

    log = logging.getLogger(LOGGER_BASENAME + '.signer')

    __SIG_METHOD = 'HmacSHA256'
    __SIG_VERSION = '2'
    __SIG_TIMESTMAP_FMT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, creds):
        self.__creds = creds
        self.__default_params = {
            'Version': API_VERSION,
            'AWSAccessKeyId': self.__creds.access_key,
            'SignatureVersion': self.__SIG_VERSION,
            'SignatureMethod': self.__SIG_METHOD,
        }
        return

    def __get_timestamp(self):
        t = datetime.datetime.utcnow()
        amz_date = t.strftime(self.__SIG_TIMESTMAP_FMT)
        return amz_date

    def __build_full_params(self, params):
        full_params = {  # Python 3.5
            **params,
            **self.__default_params, 
            **{'Timestamp':  self.__get_timestamp()}
        }
        self.log.debug('full params: {}'.format(full_params))
        
        return full_params

    def __generate_query_string(self, params):
        """
        Helper to create a request query string from a parameter dictionary
        """
        query_string_pairs = []
        for key in sorted(params.keys()):
            encoded_key = urllib.parse.quote(key, safe='')
            self.log.debug("{}".format(params[key]))
            encoded_value = urllib.parse.quote(params[key], safe='-_~')

            query_string_pairs.append("{name}={value}".format(
                name=encoded_key,
                value=encoded_value,
            ))
        query_string = '&'.join(query_string_pairs)

        return query_string

    def sign_request(self, params={}):
        # Add the common request params to the request params given
        full_params = self.__build_full_params(params)

        # Generate a query string from the full parameters dict
        query_string = self.__generate_query_string(full_params)

        # Construct the string to be signed
        string_to_sign = (
            "{method}\n"
            "{host}\n"
            "{request_uri}\n"
            "{query_string}"
                .format(
                    method=API_REQUEST_METHOD,
                    host=API_HOST,
                    request_uri=API_PATH,
                    query_string=query_string
                )
        )
        self.log.debug("\n--- string to sign ---")
        self.log.debug(string_to_sign)
        self.log.debug("--- string to sign ---\n")

        # Compute signature
        lhmac = hmac.new(
            key=self.__creds.secret_key.encode('utf-8'),
            digestmod=hashlib.sha256
        )
        lhmac.update(string_to_sign.encode('utf-8'))
        sig_bytes = lhmac.digest()
        self.log.debug("signature bytes: {}".format(sig_bytes ))

        # Encode signature for use as a parameter value
        sig_string = base64.b64encode(sig_bytes).strip().decode('utf-8')
        self.log.debug("signature string: {}".format(sig_string ))

        # Add signature parameter to query string
        query_string += (
            '&Signature=' + urllib.parse.quote(sig_string, safe='-_~')
        )

        # Return the things we made
        return SignedQueryString(
            query_string=query_string,
            signature=sig_string,
        )


class SDBClient(object):
    """
    A simple SimpleDB (heh) API client.  Uses asyncio.
    Instantiate one client per event loop. Call the request coroutine as many
    times as needed. Call close once when done.
    """

    log = logging.getLogger(LOGGER_BASENAME + '.client')

    def __init__(self, creds, loop=None):
        """
        Client constructor.
            - creds: An AWSCreds tuple (required)
            - event_loop: An asyncio event loop to use for async requests. If
              not specified, will use asyncio.get_event_loop() to get the
              default loop.
        """
        self.__creds = creds
        self.__signer = AWSv2Signer(creds)

        if loop:
            self.__event_loop = loop
        else:
            self.__evnet_loop = asyncio.get_event_loop()

        self.__http_session = aiohttp.ClientSession(loop=self.__event_loop)

        return

    async def request(self, **kwargs):
        """
        Make an api request based on the kwargs given.

        Each kwarg generates a request parameter. Arguments may be lists or
        dictionaries to express items and attributes and stuff, and will be
        flattened into query parameters automatically.

        Use the 'Action' kwarg to determine what type of call to make.

        This is a coroutine that may be yield-from'd or await'd.
        """
        # Flatten req dict to params dict
        flattened_params = self.__flatten_dict(kwargs)
        # Generate full query string and request signature
        signed_request = self.__signer.sign_request(flattened_params)
        # Headers
        headers = {
            'Content-Type': API_REQUEST_CONTENT_TYPE,
            'Host': API_HOST
        }

        # Do the request
        resp = await self.__async_request(
            API_REQUEST_METHOD,
            API_URL,
            headers=headers,
            body=signed_request.query_string
        )

        return resp

    def close(self):
        """
        Clean up internal resources when you're done with this client instance.
        TODO: Do some context manager magic
        """
        self.__http_session.close()
        return

    def __xml_to_dict(self, xml_text):
        if xml_text:
            return xmltodict.parse(xml_text)
        else:
            return {}

    def __flatten_dict(self, d, parent_key='', sep='.'):
        """
        Used to flatten a dictionary of request parameters to a single-level
        set of key-value pairs suitable for use as url-encoded query
        parameters. For example, turns this:

            {
                'Action': 'BatchDeleteAttributes',
                'DomainName': 'foo',
                'Item': [
                    {'ItemName': 'name1'},
                    {
                        'ItemName': 'name2',
                        'Attribute': [
                            {'Name': 'attr1', 'Name': 'value1'},
                            {'Name': 'attr2', 'Name': 'value2'},
                        ],
                    },
                ],
            }

        Into this:

            {
                'Action': 'BatchDeleteAttributes',
                'DomainName': 'foo',
                'Item.1.ItemName': 'name1',
                'Item.2.ItemName': 'name2',
                'Item.2.Attribute.1.Name': 'attr1',
                'Item.2.Attribute.1.Value': 'value1',
                'Item.2.Attribute.2.Name': 'attr2',
                'Item.2.Attribute.2.Value': 'value2',
            }

        Rule #2: based on http://stackoverflow.com/a/6027615
        """
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, dict):
                # Value is a dict, so recursively flatten it.
                items.extend(self.__flatten_dict(
                    v,
                    new_key,
                    sep=sep
                ).items())
            elif isinstance(v, list):
                # Value is a list, so convert it to a dict and recursively
                # flatten it.
                items.extend(self.__flatten_dict(
                    self.__list_to_dict(v, sep),
                    new_key,
                    sep=sep
                ).items())
            else:
                # Value is probably just a scalar, so include it as-is.
                items.append((new_key, v))

        return dict(items)

    def __list_to_dict(self, list_, sep):
        """
        Helper for __flatten_dict used to convert a list to a dictionary so
        that the dictionary flattening logic Just Works™ for lists.
        """
        d = {}
        for i in range(0, len(list_)):
            d[str(i+1)] = list_[i]

        return d

    async def __async_request(self, method, url, headers={}, body=None):
        """
        Do async request.
        """

        body_bytes = body.encode('utf-8')
        async with self.__http_session.request(
            method,
            url,
            headers=headers,
            data=body_bytes,
        ) as r:
            body = await r.text()
            resp = Response(
                body_text=body,
                body_dict=self.__xml_to_dict(body),
                status_code=r.status,
            ) 

        return resp

    def __sync_request(self, method, url, headers={}, body=None):
        """
        Designed to mirror __async_request, but just use requests instead.
        Just for testing.  Expect this to suffer some bitrot.
        """
        resp = None

        if method == 'POST':
            r = requests.post(
                url,
                data=body,
                headers=headers
            )
            resp = Response(
                body_text=r.text,
                body_dict=self.__xml_to_dict(r.text),
                status_code=r.status_code,
            )
        else:
            raise ValueError("Unsupported request method, '{}'.".formt(method))

        return resp
    

#### Public Helpers ##########################################################


def get_credentials(boto_profile='default'):
    """
    Helper to get aws creds from boto configuration, or the environment.
    Raises ValueError if no credentials could be found.
    """

    # Try boto profile
    profile = _get_boto_profile(boto_profile)
    if profile is not None:
        LOG.debug("Found boto profile '{}'.".format(boto_profile))
        access_key = profile.get('aws_access_key_id', None)
        secret_key = profile.get('aws_secret_access_key', None)

    # Fall back to environment variables
    if profile is None or access_key is None or secret_key is None:
        LOG.debug("Falling back to environment.".format(boto_profile))
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    # If none of that worked, blow it up
    if access_key is None or secret_key is None:
        msg = 'No aws credentials are available.'
        LOG.error(msg)
        raise ValueError(msg)

    return AWSCreds(
        access_key=access_key,
        secret_key=secret_key,
    )


#### Internal Helpers ########################################################


def _get_boto_profile(profile_name):
    """
    Helper to retrieve a boto profile object by name
    """
    session = botocore.session.get_session()
    profile = None

    for p in session.full_config['profiles']:
        if p == profile_name:
            profile = session.full_config['profiles'][p]
            break
    
    if profile is None:
        LOG.warn("AWS Profile '{}' not found.".format(profile_name))

    return profile


#### Example Code ############################################################


def _example():
    """
    Example code for using SDBClient.
    """
    raise NotImplementedError("Don't actually run this.")

    # Turn up logging for the sake of the example
    stdout = logging.StreamHandler(sys.stdout)
    def log_turnup(logger):
        logger.addHandler(stdout)
        logger.setLevel(logging.DEBUG)
    log_turnup(logging.getLogger('aiosdb'))

    # Creds
    creds = get_credentials(boto_profile='test-junk')

    # Client
    loop = asyncio.get_event_loop()
    client = SDBClient(creds, loop=loop)

    # Schedule a list domains request
    req1 = client.request(
        Action=Action.list_domains.value,
    )

    # Schedule a batch delete attributes request
    req2 = client.request(
        Action=Action.batch_delete_attributes.value,
        DomainName='jpk-whack-test',
        Item=[
            { 'ItemName': 'asdf' }
        ]
    )

    # Start event loop and run requests.
    future = asyncio.wait([req1, req2])
    done, pending = loop.run_until_complete(future)
    
    # Check results
    assert len(pending) == 0
    for task in done:
        try:
            LOG.debug("Got result: {}".format(task.result()))
        except Exception as e:
            LOG.debug("Got exception.")
            raise e

    # Clean up
    client.close()
    loop.close()

    return
