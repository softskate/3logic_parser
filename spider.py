import time
from parse import Parser


parser = Parser()

def run_spider():
    while True:
        while True:
            try:
                parser.start()
                break
            except Exception as e:
                print(f'Error occurred while scraping: {e}')
                time.sleep(5)
            time.sleep(60)
        
        time.sleep(60*60)


if __name__ == '__main__':
    while True:
        try: run_spider()
        except Exception as e: print(f'Unexpected exception occurred {e}')
        time.sleep(5)
