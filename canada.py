import requests
from bs4 import BeautifulSoup
import re
import logging
import os
# import sys
# sys.path.append("/root/central/PRE_CATS_OTTO/")
# from migration.db_class import Database_Manager
import datetime
# FERACK2 = Database_Manager('FERACK2','SOV')
# FERACK34 = Database_Manager('FERACK34','ISIN')
from collections import defaultdict

# logging.basicConfig(filename='Canada.log',filemode= '+a',format='%(asctime)s %(message)s',level = logging.INFO)



# try:
#     current_file_date=datetime.datetime.now().strftime('%Y%m%d')
#     #Create log folder if not available
#     path = '/root/central/PRE_CATS_OTTO/log_file/{}/new_issuance/'.format(current_file_date)
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     fileid = str(os.path.basename(__file__)).split('.')[0]
#     log_filename='/root/central/PRE_CATS_OTTO/log_file/'+current_file_date+'/new_issuance/'+fileid+'.log'

#     # Create and configure logger
#     logging.basicConfig(filename=log_filename,format='%(asctime)s %(message)s',filemode='a+',level = logging.INFO)
#     print('Check print statement in log file path -  {}'.format(log_filename))
            
# except (FileNotFoundError, PermissionError, FileExistsError) as e :
#     print(f"Error: {str(e)}")
    
# logging.info('***********Process Starts *************')


def call_doc_insert_operation(self,fields):

        cols = fields.keys()
        vals = fields.values()
        column_seperator = '],['
        value_seperator = "','"
        if FERACK2.numrows("select * from SOV.dbo.Sov_Italy_Documents where PublishedDate = '{}' and DocumentLink = '{}'".format(fields.get('PublishedDate',''),fields['DocumentLink'])) == 0:
            query = f"INSERT INTO [SOV].[dbo].Sov_Italy_Documents (["+ column_seperator.join(cols) +"]) VALUES ('"+ value_seperator.join(vals) +"')"
            ex = FERACK2.query(query)
            print(query)
            if 'pyodbc' not in str(ex):
                logging.info(f"Insert Failed - {ex} - {query}")
        else:
            # print(f"Already Exists - {fields}")
            pass
def BankOfCanada(soup,main_url,key_mapping):
    try:
         
        # isin_counts = defaultdict(int)
        isin_counts = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'tranche': 0})) 
        words = main_url.split('tenders-and-results/')[1].replace('/','').split('-')
        pascal_case_words = [word.capitalize() for word in words]
        pascal_case_string = ''.join(pascal_case_words)
        
        if pascal_case_string != 'CashManagementBondBuybacks' :
            
            table = soup.find('table',class_="bocss-table bocss-table--hoverable bocss-table--alternating boc_table_vert",id ='boc_table_vert_3')
        else:
            table = soup.find('table',class_="bocss-table bocss-table--hoverable bocss-table--alternating boc_table_vert",id ='boc_table_vert_4')
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        fields1 ={'Category':pascal_case_string,'Data_link':main_url,}
        td_tag = table.find('tbody').find_all('tr')
        if len(td_tag) != 0:
            for row in td_tag[-1:]:
                fields2 = {}
                columns = row.find_all('td')
                for i in range(min(len(headers), len(columns))):
                    old_key = re.sub(r'\([^)]*\)', '', headers[i]).lower().replace(' ','')
                    try:
                        new_key = key_mapping[old_key] 
                        if columns[i].text.strip() == '-':
                            fields2[new_key] = columns[i].text.strip().replace('-','')
                        else:
                            fields2[new_key] = columns[i].text.strip().replace(',','')
                        if new_key.endswith('ISIN') :
                            isin_value = columns[i].text.strip()
                            issue_date = fields2.get('IssueDate', '')
                            count = isin_counts[isin_value][issue_date]['count'] + 1
                            isin_counts[isin_value][issue_date] = count
                            # count = isin_counts[isin_value] + 1
                            # isin_counts[isin_value] = count
                            if count > 1:
                                if isin_counts[isin_value][issue_date]['tranche'] == 0 or issue_date < isin_counts[isin_value][issue_date]['tranche_issue_date']:
                                    isin_counts[isin_value][issue_date]['tranche'] = count
                                    isin_counts[isin_value][issue_date]['tranche_issue_date'] = issue_date
                            fields2[new_key] = columns[i].text.strip()
                            if str(fields2[new_key]) != '' :
                                fields2['Tranche'] =  count
                            else:
                                fields2['Tranche'] =''
                        else:
                            pass
                            
                    except :
                        pass
                    # table_data.append(fields)
                fields = {**fields1,**fields2}
                print(fields)
                logging.info(fields)
            print('Link Completed')
            logging.info('**************** Link Completed ********************')
        else:
            logging.info('No data available')
        
            
    except Exception as e:
        print(e)
        logging.info(e)
        
urls = ['regular-treasury-bills','nominal-bonds','cash-management-bills','ultra-long-bonds','real-return-bonds','cash-management-bond-buybacks','green-bonds']

for url in urls:
    
    main_url = f'https://www.bankofcanada.ca/markets/government-securities-auctions/calls-for-tenders-and-results/{url}/'
    response = requests.request('GET',main_url)
    logging.info(f'Got response - {response} for the url -{main_url}')
    soup = BeautifulSoup(response.content,'html.parser')
    # content = str(soup).split('Data available as')[1]
    # soup1 = BeautifulSoup(content,'html.parser')
    # csv_link = soup1.find('a')['href']
    key_mapping = {'auctiondate': 'AuctionDate' ,
    'biddingdeadline': 'BiddingDeadline',
    'issuedate': 'IssueDate',
    'term':'Term',
    'maturitydate': 'MaturityDate',
    'couponrate' : 'CouponRate',
    'isin': 'ISIN',
    'amount': 'Amount',
    'avgprice': 'AveragePrice',
    'allotmentprice':'AllotmentPrice',
    'allotmentyield':'AllotmentYield',
    'low5%yield' :'LowYield',
    'avgyield': 'AverageYield',
    'medianyield':'MedianYield',
    'type':'Type',
    'lowyield': 'LowYield',
    'highyield': 'HighYield',
    'coverage': 'Coverage',
    'tail': 'Tail',
    'outstandingafter': 'OutstandingAfter',
    'settlementdate':'SettlementDate',
    'amountrepurchased':'AmountRepurchased',
    'cutoffyield' : 'CutoffYield',
    'allotmentratio' : 'AllotmentRatio',
    'issueamount':'IssueAmount',
    'price' : 'Price',
    'totalamountrepurchased':'TotalAmountRepurchased',
    'totalreplacementamount':'TotalReplacementAmount',
    'cutoffspread':'CutOffSpread',
    'conversionratio':'ConversionRatio',
    'bankofcanadapurchase': 'BankOfCanadaPurchase',
    'totalsubmittedbygsd': 'TotalSubmittedByGSD',
    'totalnon-compsubmittedbygsd':'TotalNonCompSubmittedByGSD'}
    BankOfCanada(soup,main_url,key_mapping)
    
 
def BondSwitch():
        try:
            main_url = 'https://www.bankofcanada.ca/markets/government-securities-auctions/calls-for-tenders-and-results/bond-switch/'
            response = requests.request('GET',main_url)
            logging.info(f'Got response - {response} for the url -{main_url}')
            soup = BeautifulSoup(response.content,'html.parser')
            words = main_url.split('tenders-and-results/')[1].replace('/','').split('-')
            pascal_case_words = [word.capitalize() for word in words]
            pascal_case_string = ''.join(pascal_case_words)
            table = soup.find('table',class_="bocss-table bocss-table--hoverable bocss-table--alternating boc_table_vert",id ='boc_table_vert_3')
            SubCategory = 'ReplacementBond'
            values(table,SubCategory,pascal_case_string,key_mapping,main_url)
            table = soup.find('table',class_="bocss-table bocss-table--hoverable bocss-table--alternating boc_table_vert",id ='boc_table_vert_4')
            SubCategory = 'RepurchaseBond'
            values(table,SubCategory,pascal_case_string,key_mapping,main_url)
               
        except Exception as e:
            print(e)
            logging.info(e)

def values(table,SubCategory,pascal_case_string,key_mapping,main_url):
     
        isin_counts = defaultdict(int)
        headers = [header.text.strip() for header in table.find('thead').find_all('th')]
        fields1 ={'Category':pascal_case_string,'SubCategory':SubCategory,'Historical_data':main_url,}
        td_tag = table.find('tbody').find_all('tr')
        if len(td_tag) != 0:
            for row in table.find('tbody').find_all('tr'):
                fields2 = {}
                columns = row.find_all('td')
                for i in range(min(len(headers), len(columns))):
                    old_key = re.sub(r'\([^)]*\)', '', headers[i]).lower().replace(' ','')
                    try:
                        new_key = key_mapping[old_key] 
                        if columns[i].text.strip() == '-':
                            fields2[new_key] = columns[i].text.strip().replace('-','')
                        else:
                            fields2[new_key] = columns[i].text.strip().replace(',','')
                    except :
                        pass
                fields = {**fields1,**fields2}
                print(fields)
                logging.info(fields)
            print('Link Completed')
            logging.info('**************** Link Completed ********************')
        else:
            logging.info('No data available')            
                    
BondSwitch()