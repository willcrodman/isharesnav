from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from application import *

def write_securities_csv():
    binman::list_versions("chromedriver")
    ### RUN TO WRITE SEC CSV FILE
    data = {
        'ticker': [],
        'name': [],
        'href': [],
        'inception': [],
        'gross expense ratio': [],
        'net expense ratio': [],
        'net assets': []
    }

    url = "https://www.ishares.com/us/products/etf-investments#!type=ishares&fac=43511&view=keyFacts"

    driver = webdriver.Chrome()
    driver.get(url)
    exspand_securities = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
        (By.XPATH, '//*[@id="max-width-wrapper"]/ipx-table/div[3]/dw-show-more-or-less/div/button')))
    driver.execute_script("arguments[0].scrollIntoView();", exspand_securities)
    driver.execute_script("arguments[0].click();", exspand_securities)

    securities_listed = int(driver.find_element_by_xpath(
        '//*[@id="max-width-wrapper"]/ipx-table/div[1]/div/div/div/div/strong').text)
    securities = driver.find_elements_by_xpath(
        '//*[@id="max-width-wrapper"]/ipx-table/ipx-desktop-table/div/table/tbody/tr')

    for security in securities:
        data['href'].append(security.find_elements_by_tag_name('a')[0].get_attribute('href'))
        spans = security.find_elements_by_tag_name('span')
        data['ticker'].append(spans[2].text)
        data['name'].append(spans[3].text)
        divs = security.find_elements_by_tag_name('div')
        inception = datetime.datetime.strptime(divs[1].text, '%b %d, %Y').date()
        data['inception'].append(inception)
        data['gross expense ratio'].append(divs[2].text)
        data['net expense ratio'].append(divs[3].text)
        net_assets = divs[4].text
        for key, value in {',': '', 'M': '', 'K': ''}.items():
            net_assets = net_assets.replace(key, value)
        net_assets = 1000000 * float(net_assets)
        data['net assets'].append(net_assets)
    pandas.DataFrame(data=data).to_csv('//securities.csv', index=False)
    driver.quit()


def write_exchange_csv():
    ### RUN TO WRITE EXT CSV FILE (remove exchange.csv from Scrape class)
    exchange = []
    idx = 0
    df = pandas.read_csv('securities.csv')

    for index, row in df.iterrows():
        data = Application.Srape(ticker=row['ticker']).dataframes()
        for index, row in data[0]['dataframe'].iterrows():
            if row['Exchange'] not in exchange and row['Exchange'] is not None:
                exchange.append(str(row['Exchange']))
        idx += 1
        print('Completed', idx, 'of', len(df))
        print('Total Exts', len(exchange))

    df = pandas.DataFrame(data={'exchange': exchange, 'ticker': [''] * len(exchange)}, columns=['exchange', 'ticker'])
    df.to_csv('/Users/willrodman/Desktop/isharesnav/exchanges.csv')

write_securities_csv()