# KDIPWNS

In config.py file you can configure the following paremeters.
Where applicable, I have enlisted the possible values.
In PATH you need to type localpath and FILE the name of the dataset in json format as it can be extracted from BigQuery-SQL WorkSpace.

EVENT = ['checkout.loaded',transaction ,suggested_popup.closed,suggested_modal.shop_list.clicked,suggested_shop.clicked]
MONTH = ['all',3,4]
BINARY_NAME = ['Checkout',Transaction ,Suggested Popup Closed,Shop List Clicked,Suggested Shop Clicked]
PATH = 'C:/Users/efood/' 
FILE = 'efood.json'
COL = ['visitType', 'landingPage', 'appBrowser','source', 'medium']

Then in test.py you just run the process with the following filters
# process
event = True
cat = True
plot = True

The first one event = True will perform an AB Test on the event selected, period (month) defined on every variation.
The second cat = True will perform an AB Test on the event selected, period (month) defined on categorical column (COL) defined.
The third plot = True will plot event selected over various periods of time aggregations (day,day of week,weekend).
