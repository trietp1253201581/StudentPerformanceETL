from business.etl import full_process

if __name__ == '__main__':
    full_process(200, strategy='random', need_reset=True)
    full_process(1000, strategy='random')