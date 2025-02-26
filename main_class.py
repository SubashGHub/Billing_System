from Proj_Billing.log_class import Logger
from Proj_Billing.sql_class import SqlOperation
from Proj_Billing.billfunction import BillOpe


class SqlFunc:
    sql = SqlOperation()
    bill_obj = BillOpe()
    log = Logger()

    df = bill_obj.get_stock()
    print(df)

    name = input('Enter Customer Name : ')

    while True:
        try:
            choice = int(input('Choose `Id` from the list: '))
            if choice not in df.index:
                print("Invalid ID. Please choose a valid one.")
                continue
        except ValueError:
            print('Enter a valid Number.')
            continue

        qty = bill_obj.get_qty(choice)
        print(f'Added Item: {df.at[choice, 'Product']} - {qty}')
        qty_l, name_l = bill_obj.add_list(choice, qty, name)

        check = bill_obj.get_option()

        if check == 2:
            check = bill_obj.del_item(choice, qty)
        if check == 1:
            bill_report = bill_obj.bill_generate(qty_l, name_l)
            print('---------- Bill Details -----------------')
            print(bill_report)
            print(f'Total Item Purchased - {bill_report['Quantity'].sum()}')
            print('----------- Updated Stock -----------------')
            print(bill_obj.df)
            sql.create_table('Bills')
            sql.insert_data_multithreaded(bill_report, 'Bills')
            log.info('----------- Program Completed ---------------\n')
            break



