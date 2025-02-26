import pandas as pd
from datetime import datetime

df_bill = pd.read_csv('Bill Report.csv')


class BillOpe:
    filename = 'Today`s_stockList.csv'
    df = pd.read_csv(filename, index_col=0)
    qty_list = []
    name_list = []
    mrp = []
    product = []
    date = []

    def get_stock(self):
        return self.df

    def bill_generate(self, qty_l, name_l):

        df_bill['Customer_Name'] = name_l
        df_bill.Quantity = qty_l
        df_bill.MRP = self.mrp
        df_bill.Product = self.product
        df_bill.Date = self.date
        df_bill.index = range(1, len(df_bill) + 1)
        self.df.to_csv(self.filename)
        return df_bill

    def get_qty(self, choice):
        while True:
            try:
                qty = int(input('Enter Quantity : '))
            except Exception as e:
                print(f'Enter a valid Number.\n{e}')
                continue
            stock_qty = self.df.at[choice, 'Quantity']
            if qty > stock_qty:
                print(f'Only avail stock: {stock_qty}')
                continue
            return qty

    def get_option(self):
        while True:
            try:
                check = int(
                    input('Enter Option.\n      1 --> Bill Generate.\n      2 --> To delete last item.\nEnter '
                          'Any other Number  --> To add more: '))
                return check
            except ValueError:
                print("Enter a valid Number.")
                continue

    def add_list(self, choice, qty, name):
        self.mrp.append(self.df.at[choice, 'MRP'])
        self.product.append(self.df.at[choice, 'Product'])
        self.qty_list.append(qty)
        self.name_list.append(name)
        self.date.append(datetime.now().strftime("%Y-%m-%d %H:%M"))
        self.df.at[choice, 'Quantity'] = self.df.at[choice, 'Quantity'] - qty
        return self.qty_list, self.name_list

    def del_item(self, choice, qty):
        self.mrp.pop()
        self.product.pop()
        self.qty_list.pop()
        self.name_list.pop()
        self.date.pop()
        self.df.at[choice, 'Quantity'] += qty
        print(f'Deleted Item: {self.df.at[choice, 'Product']} - {qty}')
        while True:
            try:
                check = int(input("Enter Option.\n      1 --> Bill Generate.\nEnter Any other Number  --> To add more "
                                  ": "))
                return check
            except ValueError:
                print('Please Enter a valid Number.')
                continue

