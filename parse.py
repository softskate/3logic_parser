import base64
from datetime import datetime, timedelta
import imaplib
import email
import shutil
import os
import time
from database import Product, App, Crawl
import pandas as pd
from keys import EMAIL_ACCOUNT, EMAIL_PASSWORD


IMAP_SERVER = 'imap.ya.ru'
SAVE_FOLDER = 'prices'

class Parser:
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    def save_attachment(self, part, folder):
        if not os.path.isdir(folder):
            os.makedirs(folder)
        filename = part.get_filename()
        filename = self.decode(filename)
        if filename:
            filepath = os.path.join(folder, filename)
            with open(filepath, 'wb') as f:
                f.write(part.get_payload(decode=True))
            return filepath
    
    @staticmethod
    def decode(text):
        out = ''
        for part in text.split('?='):
            part = part.split('=?', 1)
            if len(part) == 2:
                out += part[0]
                part = part[1]
                if '?B?' in part:
                    encoder, enc = part.split('?B?', 1)
                    decoded_bytes = base64.b64decode(enc)
                    enc = decoded_bytes.decode(encoder)
                    out += enc
            else:
                out += part[0]

        return out


    def process_email_message(self, msg):
        done = False
        for part in msg.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            filepath = self.save_attachment(part, SAVE_FOLDER)
            if filepath.endswith('.zip'):
                shutil.unpack_archive(filepath, SAVE_FOLDER)
                filepath = filepath.rsplit('.', 1)[0] + '.xls'
                if os.path.isfile(filepath):
                    self.parse(filepath)
                    done = True
            
        return done


    def start(self):
        complated = False
        self.mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
        self.mail.select('inbox')

        status, data = self.mail.search(None, 'UNSEEN')
        mail_ids = data[0].split()

        for mail_id in mail_ids:
            status, msg_data = self.mail.fetch(mail_id, '(RFC822)')
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            subject = msg.get('subject', 'No Subject').strip()
            subject = self.decode(subject)
            sender = msg.get('From')
            sender = self.decode(sender)
            
            self.mail.store(mail_id, '-FLAGS', '\\Seen')
            if 'i.paliutina@3l.ru' in sender:
                self.mail.store(mail_id, '+FLAGS', '\\Seen')
                data = self.process_email_message(msg)
                if data:
                    complated = True

        self.mail.logout()
        return complated


    def parse(self, file_path):
        
        old_crawlers = Crawl.select().where(Crawl.created_at < (datetime.now() - timedelta(days=3)))
        dq = (Product
            .delete()
            .where(Product.crawlid.in_(old_crawlers)))
        dq.execute()

        Crawl.delete().where(Crawl.finished==False)
        
        df = pd.read_excel(file_path)
        df = df.where(pd.notna(df), None)
        row_data = df.iloc[3].tolist()
        date = df.iloc[1].tolist()[:2]
        date = datetime.strptime('T'.join(date), '%d.%m.%YT%H:%M:%S')

        translator = {
            'Раздел': 'cat1',
            'Подраздел 1': 'cat2',
            'Подраздел 2': 'cat3',
            'Артикул': 'productId',
            'Наименование': 'name',
            'Бренд': 'brandName',
            'Наличие': 'in_stock',
            'Руб.': 'price'
        }

        det_head = {}
        data = []
        headers = {}
        for n, cell in enumerate(row_data):
            if cell in translator:
                headers[n] = translator[cell]
            else:
                det_head[n] = cell

        appid = App.create(name='3Logic')
        crawlid = Crawl.create(created_at=date)
        for x, row in list(df.iterrows())[4:]:
            row_data = {}
            details = {}
            for i in range(len(row)):
                if i in headers:
                    row_data[headers[i]] = row.iloc[i]
                
                elif i in det_head:
                    details[det_head[i]] = str(row.iloc[i])
            row_data['category'] = ' - '.join([row_data.pop(f'cat{x}') for x in range(1,4) if row_data[f'cat{x}']])
            row_data['details'] = details
            row_data['appid'] = appid
            row_data['crawlid'] = crawlid
            if not row_data['name']: continue
            Product.create(**row_data)
            time.sleep(.001)


        crawlid.finished = True
        crawlid.save()
        try: os.remove(file_path)
        except Exception as e: print("Error removing", e)
        return data