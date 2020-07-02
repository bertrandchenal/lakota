# TODO compare local vs s3/minio perfs
import s3fs

s3 = s3fs.S3FileSystem(
    anon=False,
    client_kwargs={'endpoint_url': 'http://192.168.0.104:9000'},
    key='minioadmin',
    secret='minioadmin')

s3.ls()
