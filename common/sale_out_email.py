from datetime import datetime

from tkmail import Email
from common.config import email_config


def send_sale_out_email(df):
    email_to = email_config['to_email']
    email_cc = email_config['cc_email']
    email_bcc = email_config['bcc_email']
    email_text = "<p>Doanh thu theo nhân viên đặt bàn</p>"

    if len(df) > 0:
        email_text += "<table border='1'>"
        email_text += f"<tr><th>nhân viên</th> <th>doanh thu ngày</th> <th>doanh thu tuần</th> <th>doanh thu tháng</th>"
        for row in df.to_dict(orient='records'):
            email_text += f"<tr><td>{row['employee']}</td> <td>{row['revenue']}</td><td>{row['revenue_wtd']}</td> <td>{row['revenue_mtd']}</td>"
        email_text += "</table>"

    subject = "[{}] Báo cáo doanh số".format(datetime.now())

    email = (
        Email(
            username=email_config['username'],
            password=email_config['password'],
            subject=subject,
            sender='report team',
            to=email_to,
            cc=email_cc,
            bcc=email_bcc,
        )
        .html(email_text)
    )
    email.send().retry(times=5)

