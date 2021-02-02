import threading, time, wget, csv, itertools, \
    pandas, os, logging, sys, datetime, ntplib, pytz, getpass

class Application:
    access_permission = 0o755

    def __init__(self):
        logging.getLogger()
        self.master_dir = f'/Users/{getpass.getuser()}/Desktop/isharesnavrequest/'
        os.system('clear')
        print('''
***********************************
***** IShares NAV Application *****
***********************************

Copyright (C) 2020 Will Rodman
<wrodman@tulane.edu>
        ''')

    def __call__(self, *args, **kwargs):
        time.sleep(3)
        os.system('clear')
        print(f'''
Application master directory: {self.master_dir}
_____________________________
[1] Use current master directory
[2] Use new master directory
        ''')
        statement = input()
        self.execute_statement(self.programs, self.write_master_dir, statement=statement)

    class NTP:
        @staticmethod
        def dt():
            return datetime.datetime.now(pytz.timezone('US/Eastern'))

        @staticmethod
        def timedelta():
            client = ntplib.NTPClient()
            response = client.request('pool.ntp.org')
            timedelta = abs(response.tx_time - response.orig_time)
            if timedelta > 1:
                logging.warning('Network datetime not in-sync')
            return round(timedelta, 3)

    class Request(NTP):
        def __init__(self, tickers, master_dir):
            self.master_dir = master_dir
            try:
                os.makedirs(self.master_dir, Application.access_permission)
            except FileExistsError:
                pass
            except:
                logging.error(f'{self.master_dir} does not exist')
            if type(tickers) == list:
                self.tickers = tickers
            else:
                sys.exit(1)

        def __call__(self, *args, **kwargs):
            dataframe = pandas.read_csv('securities.csv')
            for index, row in dataframe.iterrows():
                if row['ticker'] in self.tickers:
                    url = row['href'] + \
                          '/1467271812596.ajax?fileType=csv&fileName={}_holdings&dataType=fund'.format(
                              row['ticker'])
                    security_dir = self.master_dir + row['ticker'] + '/'
                    try:
                        os.makedirs(security_dir, Application.access_permission)
                    except FileExistsError:
                        pass
                    except:
                        logging.error(f'{security_dir} does not exist')
                    timestamp = int(round(super().dt().timestamp(), 0))
                    filename = '{}.csv'.format(timestamp)
                    wget.download(url, security_dir + filename)
                    print(f"\nCompleted request for {row['ticker']}", end='\n'
                          f"Request {1 + self.tickers.index(row['ticker'])} of {len(self.tickers)}")
                    time.sleep(5)
            return True

    class Srape:
        def __init__(self, ticker, master_dir):
            self.filepaths = []
            self.master_dir = master_dir
            self.ticker = ticker
            self.exchanges = pandas.read_csv('exchanges.csv') \
                .set_index('exchange').to_dict()['mic']
            security_path = os.path.join(master_dir, '{}/'.format(ticker))
            if os.path.exists(security_path) == True:
                for file in os.listdir(security_path):
                    if file.endswith(".csv"):
                        self.filepaths.append(os.path.join(security_path, file))
                        os.path.exists(self.filepaths[0])
            else:
                logging.error(f'Directory not found for {self.ticker}'
                              f'{FileNotFoundError}')
                sys.exit(1)

        @staticmethod
        def float_dtype(string):
            return float(string.replace(',', ''))

        def dataframes(self):
            intervals = []
            for filepath in self.filepaths:
                with open(filepath) as file:
                    data = csv.reader(file, delimiter=',')
                    for index, row in enumerate(itertools.islice(data, 9)):
                        try:
                            if index == 1:
                                date = str(datetime.datetime.strptime(row[1], "%b %d, %Y").date())
                                pass
                            if index == 3:
                                shares = self.float_dtype(row[1])
                        except IndexError as error:
                            logging.error(f'Illegal heading above CSV for {self.ticker} file {file.name}'
                                          f'{error}')
                            sys.exit(2)
                    try:
                        dataframe = pandas.DataFrame(data=data)
                        header = dataframe.iloc[0]
                        dataframe = dataframe[1:]
                        dataframe.columns = header
                    except:
                        logging.error(f'Illegal CSV header for {self.ticker} file {file.name}')
                        sys.exit(3)
                    try:
                        dataframe = dataframe.drop(columns=['Name', 'Market Value', 'Maturity', 'Currency',
                                                            'Location', 'Weight (%)', 'Price', 'FX Rate'])
                        for index, row in dataframe.iterrows():
                            row['Shares'] = self.float_dtype(row['Shares'])
                            row['Notional Value'] = self.float_dtype(row['Notional Value'])
                            def write_exchange(exchange):
                                dataframe.loc[index, 'Exchange'] = exchange
                            if not row['Ticker'] in ['-', '']:
                                write_exchange(self.exchanges[row['Exchange']])
                            else:
                                write_exchange('None')
                    except:
                        nav_dataframe = pandas.DataFrame(data={'NAV Date': ([date] * len(dataframe)),
                                                               'NAV Shares': ([shares] * len(dataframe))},
                                                         columns=['NAV Date', 'NAV Shares'])
                    file.close()
                    dataframe = dataframe.join(nav_dataframe)
                    #print(f'\nScrape completed for file {file.name}')
                    intervals.append(dataframe)
            return intervals

        def concatenate_dataframes(self):
            columns = list(['Shares'])
            intervals = self.dataframes()
            try:
                for interval in intervals:
                    interval['Header'] = interval['Exchange'] \
                                             .astype(str) + '$' + interval['Ticker'].astype(str)
                    new_columns = list(set(interval['Header']) - set(columns))
                    columns += new_columns
                df = pandas.DataFrame(columns=columns)
            except:
                logging.error('failed to compile dataframe headers')
                sys.exit(4)
            try:
                for interval in intervals:
                    shares = [0] * len(df.columns)
                    date = interval['NAV Date'].iloc[0]
                    nav_shares = interval['NAV Shares'].iloc[0]
                    for index, row in interval.iterrows():
                        header = df.columns.get_loc(row['Header'])
                        if str(row['Header']).split('$')[0] != 'None':
                            shares[header] = row['Shares']
                        else:
                            shares[header] = row['Notional Value']
                    shares[df.columns.get_loc('Shares')] = nav_shares
                    df.loc[date] = shares
            except:
                logging.error('failed to compile dataframe indexes')
                sys.exit(5)
            df.sort_index(inplace=True)
            df.to_csv(path_or_buf='{}{}.csv'.format(self.master_dir, self.ticker))
            return True


#Not done
    class Clean:
        def __init__(self, tickers, master_dir):
            self.filepaths = []
            self.master_dir = master_dir
            self.tickers = tickers
            for ticker in tickers:
                security_path = os.path.join(master_dir, '{}/'.format(ticker))
                if os.path.exists(security_path) == True:
                    for file in os.listdir(security_path):
                        if file.endswith(".csv"):
                            self.filepaths.append(os.path.join(security_path, file))
                            os.path.exists(self.filepaths[0])
                else:
                    logging.error(f'Directory not found for {ticker}'
                                f'{FileNotFoundError}')
                    sys.exit(1)

        def remove_illegal_files(self):
                security_path = self.master_dir + "/" + ticker

        def remove_duplicate_files(self):
            pass


        def get_date(self):
            pass

    @classmethod
    def module_warning(cls):
        logging.getLogger()
        name = Application.__name__
        module = Application.__module__
        logging.warning(f'{name} is not running from {module}.py')
        return cls()

    def write_master_dir(self):
        os.system('clear')
        print(f'''
Enter new master directory:
___________________________
                ''')
        statement = input()
        if not os.path.exists(statement):
            self.master_dir = statement
            self.programs()

    def execute_statement(self, *funcs, statement):
        try:
            statement = abs(int(statement))
            funcs[statement - 1]()
        except (TypeError, SyntaxError, IndexError) as error:
            logging.error(error)
            self.__call__()
        except:
            logging.error('unknown error')
            sys.exit(2)

    def programs(self):
        os.system('clear')
        print(f'''
Application programs:
_____________________
[1] Concatenate NAVs 
[2] Request NAVs 
[3] Clean {self.master_dir}
        ''')
        statement = input()
        self.execute_statement(self.concat, self.request, statement=statement)

    def write_tickers(self):
        os.system('clear')
        print(f'''
Enter array of ETF tickers to process:
(Enter empty array to process all ETFs)
_______________________________________
            ''')
        statement = input()
        try:
            statement = list(eval(statement))
            if statement == []:
                return pandas.read_csv('securities.csv')['ticker'].tolist()
            else:
                return statement
        except (SyntaxError, NameError) as error:
            logging.error(error)
            self.__call__()
        except:
            logging.error('cannot accept list of tickers')
            sys.exit(3)

    def request(self):
        tickers = self.write_tickers()
        dt = self.NTP.dt()
        requests = self.Request(tickers=list(tickers), master_dir=self.master_dir)
        close = dt.replace(hour=16, minute=0, second=0)
        os.system('clear')
        print(f'''
***************************
* Running Request Program *
***************************

Network Timedelta (seconds): {self.NTP.timedelta()}
            ''', end='\n')
        while True:
            if close < dt:
                print("\n       *** Fetching Requests ***\n")
                while requests():
                    print('\n       *** Requests Complete ***')
                    self.__call__()
            else:
                print("\n       Program will fetch requests after 16:00:00 EST"
                      f"\n      Market Clock: {dt.strftime('%X')} EST")
                time.sleep(60)

    def concat(self):
        def batch_list(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        def header():
            os.system('clear')
            print(f'''
*********************************
* Running Concatenation Program *
*********************************
                    ''', end='\n')

        batchs = list(batch_list(self.write_tickers(), 45))
        header()
        threads = list()
        percents = [0] * len(batchs)
        for idx, batch in enumerate(batchs):
            def target(*args):
                global percent
                for ticker in batch:
                    if self.Srape(ticker=ticker, master_dir=self.master_dir).concatenate_dataframes():
                        percents[args[0]] = round((batch.index(ticker) + 1) / len(batch) * 100, 0)
                del threads[args[0]]
            threads.append(threading.Thread(target=target, args=(idx,)))
            print(f"    THREAD {idx + 1} established for {len(batch)} tasks from memory")
        for t in threads: t.start()
        while threads:
            time.sleep(2), header()
            for idx, percent in enumerate(percents):
                print(f"    THREAD {idx + 1}: {percent}% tasks completed")
        self.__call__()

if __name__ == '__main__':
    execute = Application()
    execute()
else:
    Application.module_warning()
