from business.etl import etl_process
import schedule
import time
    
import logging
# Thiết lập config cho log
logging.basicConfig(filename='business/etl_log.log', level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Các biến lưu giữ trạng thái của job hiện tại
job_cnt = 0
stop = False

def combined_etl_process(limits: int|None = None):
    """
    Một quy trình ETL tự động được thực thi.
    Args:
        limits (int or None): Giới hạn số lần thực hiện quy trình, nếu là
        None thì không có giới hạn. Giá trị mặc định là None
    """
    global job_cnt
    global stop
    job_cnt += 1
    logging.info(f'<<Job {job_cnt}>>')
    etl_process(
        load_for=2000,
        strategy='random',
        need_reset=False
    )
    if limits is not None:
        if job_cnt >= limits:
            logging.info(f"Execution count has reached {limits}. Cancelling the job.")
            stop = True
            return schedule.CancelJob

# Giả thiết chạy ETL 5 lần, mỗi lần tải mẫu 2000 hàng dữ liệu
if __name__ == '__main__':
    job = schedule.every().minute.do(lambda: combined_etl_process(5))
    while True:
        schedule.run_pending()
        time.sleep(1)
        if stop:
            schedule.cancel_job(job)
            break
    logging.info('Done!!!')
        