from gevent import monkey
import gevent
import urllib.request
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
def downloader(name,img_url):
    req=urllib.request.urlopen(img_url)
    img_content = req.read()#存图片
    with open (name,"wb") as f:#读图片，写东西
        f.write(img_content)

def main():
    gevent.joinall([gevent.spawn(downloader,"2.jpg","https://www.scu.edu.cn/__local/D/1F/02/CA6B3D9F60FFCF7CEA111088EA3_BE1DA4B8_C4F0F.jpg"),
                    gevent.spawn(downloader,"3.jpg","https://www.scu.edu.cn/__local/1/10/99/5929EA573EE8D1AEC206A073502_119F7220_39553.jpg")])

if __name__ == "__main__":
    main()