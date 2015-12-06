### aiosdb

A (really) thin wrapper around the AWS SimpleDB service using
[aiohttp](http://aiohttp.readthedocs.org/en/stable/).

#### Example

```python
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
        print("Got result: {}".format(task.result()))
    except Exception as e:
        print("Got exception.")
        raise e

# Clean up
client.close()
loop.close()
```


#### Disclaimers

- Requires Python 3.5
- Only supports us-east-1 region out of laziness, but could be set up to touch
other regions.  (See constants at top of file.)
- Only tested with a subset of SDB's API actions. Again, this is mostly out of
laziness, and other ones will probably work fine. (See the `Action` enum for
adding them.) The actions tested so far are:
    - ListDomains
    - BatchDeleteAttributes
    - BatchPutAttributes
- Requires botocore just to get credentials from your profile.
- The request method's interface kind of sucks. It accepts kwargs that are
used to directly build the request parameters [as seen here](http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDB_API_ParametersDescription.html).
The problem being: the natural thing to write in python would be `Attributes=[foo, bar, ...]`,
but you have to drop the plural so it'll be flattened to `Attribute.1`,
`Attribute.2`, etc.  boto and friends do this for you, but this doesn't.


