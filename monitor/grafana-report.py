# coding: utf8

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import parseaddr, formataddr
import time as Time
import re
import shutil
import os

sender = 'Grafana<xxxxxxxxx@163.com>'
to_address = ['xxxxxxxxx@163.com']
username = 'xxxxxxxxx@163.com'
password = 'xxxxxxxxx' # SMTP授权码
smtpserver = 'xxxx.163.com:1234'
sourcefile= '/etc/curve/monitor/grafana/report/report.tex'
imagedir= '/etc/curve/monitor/grafana/report/images/'
pdfpath= '/etc/curve/monitor/grafana/report/report.pdf'
clustername = '【CURVE】xxxxxxxxx'
grafanauri = '127.0.0.1:3000'
reporteruri = '127.0.0.1:8686'
dashboardid = 'xxxxxxxxx'
apitoken = 'xxxxxxxxx'

def get_images():
    image_name_list = []
    file = open(sourcefile, 'r')
    line = file.readline()
    while line:
        # print (line)
        prefix_image_name = re.findall(r'image\d+', line)
        if prefix_image_name:
            print (prefix_image_name)
            image_name_list.append(prefix_image_name[0])
        line = file.readline()
    file.close()

    return image_name_list

def getMsgImage(image_name):
    file_name = imagedir+image_name+'.png'
    print (file_name)
    fp = open(file_name, 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    msgImage.add_header('Content-ID', image_name)
    msgImage.add_header("Content-Disposition", "inline", filename=file_name)
    return msgImage

def attach_body(msgRoot):
    image_list = get_images()

    image_body = ""
    for image in image_list:
        image_body += ('<img src="cid:%s" alt="%s">' % (image, image))
        msgRoot.attach(getMsgImage(image))

    html_str = '<html><head><style>#string{text-align:center;font-size:25px;}</style></head><body>%s</body></html>' % (image_body)

    mailMsg = """
    <p>可点击如下链接在grafana面板中查看（若显示混乱，请在附件pdf中查看）</p>
    <p><a href="http://%s">grafana链接</a></p>
    """ % (grafanauri)
    mailMsg += html_str
    print(mailMsg)
    content = MIMEText(mailMsg,'html','utf-8')
    msgRoot.attach(content)

# 发送dashboard日报邮件
def send_mail():
    time_now = int(Time.time())
    time_local = Time.localtime(time_now)
    dt = Time.strftime("%Y%m%d",time_local)

    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = '%s集群监控日报-%s' % (clustername, dt)
    msgRoot['From'] = sender
    msgRoot['To'] = ",".join( to_address ) # 发给多人

    # 添加pdf附件
    pdf_attach = MIMEText(open(pdfpath, 'rb').read(), 'base64', 'utf-8')
    pdf_attach["Content-Type"] = 'application/octet-stream'
    # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
    pdf_attach["Content-Disposition"] = 'attachment; filename="reporter-{}.pdf"'.format(dt)
    msgRoot.attach(pdf_attach)

    # 添加正文
    attach_body(msgRoot)

    smtp = smtplib.SMTP_SSL(smtpserver)
    smtp.login(username, password)
    smtp.sendmail(sender, to_address, msgRoot.as_string())
    smtp.quit()

def clear():
    shutil.rmtree(imagedir)
    os.mkdir(imagedir)
    os.chmod(imagedir, 0777)

def generate_report():
    downloadcmd = (
        "wget -O %s "
        "http://%s/api/v5/report/%s?apitoken=%s"
        "\&from=now-24h\&to=now"
    ) % (pdfpath, reporteruri, dashboardid, apitoken)
    print(downloadcmd)
    os.system(downloadcmd)

def main():
    generate_report()
    send_mail()
    clear()

if __name__ == '__main__':
    main()
