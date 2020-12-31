from datetime import datetime

from tkmail import Email
from common.config import email_config


def send_email(dispose, sale_order):
    email_to = email_config['to_email']
    email_cc = email_config['cc_email']
    email_bcc = email_config['bcc_email']
    email_text = "<p>Danh sách nghi ngờ sai phạm</p>"

    if len(dispose) > 0:
        email_text += f" <p> danh sách hủy hàng </p> <table border='1'>"
        email_text += f"<tr><th>thời gian</th> <th>nhân viên</th> <th>bàn</th> <th>mặt hàng</th> <th>lý do</th>"
        for row in dispose.to_dict(orient='records'):
            email_text += f"<tr><td>{row['tran_at']}</td> <td>{row['employee']}</td><td>{row['table']}</td> <td>{row['product']}</td> <td> {row['reason']}</td>"
        email_text += "</table>"
    if len(sale_order) > 0:
        email_text += f" <p> danh sách thanh toán trái phép</p> <table border='1'>"
        email_text += f"<tr><th>thời gian</th> <th>mã đơn hàng</th> <th>nhân viên</th> <th>tổng tiền</th>"
        for row in sale_order.to_dict(orient='records'):
            email_text += f"<tr><td>{row['tran_at']}</td> <td>{row['order_code']}</td> <td>{row['employee']}</td> <td> {row['amount']}</td>"
        email_text += "</table>"

    subject = "[{}] [Fraud] Báo cáo nghi ngờ".format(datetime.now())

    email = (
        Email(
            username=email_config['username'],
            password=email_config['password'],
            subject=subject,
            sender='FRAUD_TEAM',
            to=email_to,
            cc=email_cc,
            bcc=email_bcc,
        )
        .html(email_text)
    )
    email.send().retry(times=5)

