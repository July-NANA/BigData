import boto3
from boto3.s3.transfer import TransferConfig

ACCESS_KEY = 'C802139A1D6D3356ABCD'
SECRET_KEY = 'WzU0NDRGMDc1Q0I5OTA5QjI0QTk5RThENEU0RDY4MTI4MkNDM0M3ODJd'
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    # 下面给出一个endpoint_url的例子
    endpoint_url="http://scuts3.depts.bingosoft.net:29999"
)

MB=1024**2
config=TransferConfig(multipart_threshold=5*MB)

Bucket="fanyuhang"
name="Python编程：从入门到实践.pdf"
keyname=name
filename="E:\\学习资料\\大数据实训\\下载\\python\\"+name

path="E:\\学习资料\\大数据实训\\下载\\python"
# s3=boto3.client("s3")
# s3.upload_file("E:\\学习资料\\大数据实训\\上传\\picture.jpg", Bucket, "picture.jpg")

s3.download_file(Bucket, keyname,filename,Config=config )
