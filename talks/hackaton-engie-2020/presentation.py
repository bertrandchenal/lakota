from hashlib import sha1

from pandas import DataFrame

# important_info = b"Hello world"
# print(sha1(important_info).hexdigest())
# # -> 7b502c3a1f48c8609ae212cdfb639dee39673f5e

# another_info = b"Hello world!"
# print(sha1(another_info).hexdigest())
# # -> d3486ae9136e7856bc42212385ea797094475802


# db = {
#     b'7b502c3a1f48c8609ae212cdfb639dee39673f5e': important_info,
#     b'd3486ae9136e7856bc42212385ea797094475802' : another_info,
# }

# cheksum = sha1(b'7b502c3a1f48c8609ae212cdfb639dee39673f5e')
# cheksum.update(b'd3486ae9136e7856bc42212385ea797094475802')
# print(cheksum.hexdigest())
# # -> 4812b3c515b3c453d399fb95bb5b0a261c3542c9

# db[b'4812b3c515b3c453d399fb95bb5b0a261c3542c9'] = [
#     b'7b502c3a1f48c8609ae212cdfb639dee39673f5e',
#     b'd3486ae9136e7856bc42212385ea797094475802'
# ]

# some_new_info = b'Bye'
# print(sha1(some_new_info).hexdigest())
# # -> f792424064d0ca1a7d14efe0588f10c052d28e69
# db[b'f792424064d0ca1a7d14efe0588f10c052d28e69'] = some_new_info

# cheksum.update('f792424064d0ca1a7d14efe0588f10c052d28e69'.encode())
# print(cheksum.hexdigest())
# # -> 87230955960e9c54c6cc43db35ad91602bdfdd73

# db[b'87230955960e9c54c6cc43db35ad91602bdfdd73'] = [
#     b'7b502c3a1f48c8609ae212cdfb639dee39673f5e',
#     b'd3486ae9136e7856bc42212385ea797094475802',
#     b'f792424064d0ca1a7d14efe0588f10c052d28e69',
# ]
# log = [
#     b'4812b3c515b3c453d399fb95bb5b0a261c3542c9',
#     b'87230955960e9c54c6cc43db35ad91602bdfdd73',
# ]


# commit = b'87230955960e9c54c6cc43db35ad91602bdfdd73'
# for key in db[commit]:
#     msg = db[key]
#     print(f'{key}: {msg}')

# # -> b'Hello world | Hello world!'
# #    b'Hello world | Hello world! | Bye'

df = DataFrame(
    {
        "timestamp": [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-04",
            "2020-01-05",
        ],
        "value": [1, 2, 3, 4, 5],
    }
)
print(df)

print(sha1(df["timestamp"].to_numpy()).hexdigest())
print(sha1(df["value"].to_numpy()).hexdigest())
