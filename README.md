# config.py

In config.py file you can configure the following paremeters. <br/>
Where applicable, I have enlisted the possible values. <br/>
In __PATH__ you need to type __localpath__ and __FILE__ the name of the dataset in __json__ format as it can be extracted from __BigQuery-SQL WorkSpace__.

EVENT = ['checkout.loaded',transaction ,suggested_popup.closed,suggested_modal.shop_list.clicked,suggested_shop.clicked] <br/>
MONTH = ['all',3,4] <br/>
BINARY_NAME = ['Checkout',Transaction ,Suggested Popup Closed,Shop List Clicked,Suggested Shop Clicked] <br/>
PATH = 'C:/Users/'  <br/>
FILE = 'data.json' <br/>
COL = ['visitType', 'landingPage', 'appBrowser','source', 'medium'] <br/>


# test.py
Then in test.py you just run the process with the following filters <br/>
event = True <br/>
cat = True <br/>
plot = True <br/>

The first one event = True will perform an AB Test on the event selected, period (month) defined on every variation. <br/>
The second cat = True will perform an AB Test on the event selected, period (month) defined on categorical column (COL) defined. <br/>
The third plot = True will plot event selected over various periods of time aggregations (day,day of week,weekend). <br/>

# requirements.txt

Both data pipeline and ipynb file were created with the installation of the libraries and their respective versions.
