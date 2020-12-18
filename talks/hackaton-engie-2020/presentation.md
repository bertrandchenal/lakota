
# Lakota 

A git-like storage for timeseries

---

## How does git keep track of your data ?

--- 

### Checksums

A checkum is a function that transform a message into a number.


Example


``` python
from hashlib import sha1

important_info = b"Hello world"
print(sha1(important_info).hexdigest())
# -> 7b502c3a1f48c8609ae212cdfb639dee39673f5e

another_info = b"Hello world!"
print(sha1(another_info).hexdigest())
# -> d3486ae9136e7856bc42212385ea797094475802
```

---

Properties:

- Fast and Deterministic
- Any change to the message will generate a different result
- Impossible to revert

---

Reverting is impossible but we can store the information:

``` python
db = {
    b'7b502c3a1f48c8609ae212cdfb639dee39673f5e': important_info, # "Hello world"
    b'd3486ae9136e7856bc42212385ea797094475802': another_info,  # "Hello world!"
}
```

So we can query it:

``` python
print(db[b'7b502c3a1f48c8609ae212cdfb639dee39673f5e'])
# -> b'Hello world'
```

---

Moreover we can compute a digest of digests

``` python
cheksum = sha1(b'7b502c3a1f48c8609ae212cdfb639dee39673f5e')
cheksum.update(b'd3486ae9136e7856bc42212385ea797094475802')
print(cheksum.hexdigest())
# -> 4812b3c515b3c453d399fb95bb5b0a261c3542c9

db[b'4812b3c515b3c453d399fb95bb5b0a261c3542c9'] = [
    b'7b502c3a1f48c8609ae212cdfb639dee39673f5e',
    b'd3486ae9136e7856bc42212385ea797094475802'
]
```
(and store it)

---

Let's add a new message

``` python
some_new_info = b'Bye'
print(sha1(some_new_info).hexdigest())
# -> f792424064d0ca1a7d14efe0588f10c052d28e69
db[b'f792424064d0ca1a7d14efe0588f10c052d28e69'] = some_new_info
```

And update our checksum of checksum

``` python
cheksum.update(b'f792424064d0ca1a7d14efe0588f10c052d28e69')
print(cheksum.hexdigest())
# -> 87230955960e9c54c6cc43db35ad91602bdfdd73

db[b'87230955960e9c54c6cc43db35ad91602bdfdd73'] = [
    b'7b502c3a1f48c8609ae212cdfb639dee39673f5e',
    b'd3486ae9136e7856bc42212385ea797094475802',
    b'f792424064d0ca1a7d14efe0588f10c052d28e69',
]
```

---

Our minimal git implementation is nearly complete.


``` python
log = [
    b'4812b3c515b3c453d399fb95bb5b0a261c3542c9', # First commit
	b'87230955960e9c54c6cc43db35ad91602bdfdd73', # Second commit
]
```
The log contains only the digests of digests (not the original messages)


---

For example the equivalent of a checkout is:


``` python
commit = b'87230955960e9c54c6cc43db35ad91602bdfdd73'
for key in db[commit]:
    msg = db[key]
    print(f'{key}: {msg}')

# -> b'7b502c3a1f48c8609ae212cdfb639dee39673f5e': b'Hello world'
#    b'd3486ae9136e7856bc42212385ea797094475802': b'Hello world!'
#    b'f792424064d0ca1a7d14efe0588f10c052d28e69': b'Bye'
```

---

So with the above concept, we see how we can:

- Detect a file modification.
- Abstract the state of a large collection of file into one checksum
- Deduplicate content
- Ensure data consistency

---

### Merkle tree

This checksum of checksums (or checksum of checksums of checksums ...) is
called a Merkle Tree. 


---

## What about timeseries ?

---

A timeseries is a dataframe with at least two columns


``` python
df = DataFrame({
    'timestamp': ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05'],
    'value': [1, 2, 3, 4, 5],
})
print(df)

# ->
#        timestamp  value
#    0  2020-01-01      1
#    1  2020-01-02      2
#    2  2020-01-03      3
#    3  2020-01-04      4
#    4  2020-01-05      5
```

---

We compute one digest per column:

``` python
print(sha1(df['timestamp'].to_numpy()).hexdigest())
# -> 35926b0130f74f08c6020af537e765258deeef03

print(sha1(df['value'].to_numpy()).hexdigest())
# -> 7bfa1c0042237357f6eb89ec07d5e8a89e2d1d0e
```

And combine those to have a digest of our dataframe:

``` python
sha1(b'3592...').update(b'7bf...')
```

---

And then do the same operations for a collection of
dataframe:

``` python
second_df = DataFrame({
    'timestamp': ['2020-01-06', '2020-01-07', '2020-01-08', '2020-01-09', '2020-01-10'],
    'value': [6, 7, 8, 9, 10],
})
```

(rince and repeat)

Remarks:

- Each dataframe is a piece of a timeseries and is represented by a checksum.
- So the timeseries state is also abstracted by one checksum.

---

### Conclusions

If we add two ingredients:

- an efficient compression of the columns data
- a remote storage

We get a timeseries database that offer the following properties:

- Space efficiency
- Ease of synchronisation, ease of caching
- Concurrent and atomic access (reads and writes)
- Ability to "rewind" the database

All those ideas and more have been implement in [Lakota](https://github.com/bertrandchenal/lakota)

---

### Demo Time

Web UI

Pull dataset from CLI

---

## Thank you.  Questions ?

