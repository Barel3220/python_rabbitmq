from producer import Producer


# creating a main to activate producer properly.
def main():
    """
    holding a list of bytes including 'path', 'type' and 'table_name'
    to activate the first module - Producer
    """
    producer = Producer()

    list_of_files = [
        b"C:/Users/barel/Desktop/Files/invoices_2009.json JSON invoices_2009",
        b"C:/Users/barel/Desktop/Files/invoices_2010.json JSON invoices_2010",
        b"C:/Users/barel/Desktop/Files/invoices_2011.json JSON invoices_2011",
        b"C:/Users/barel/Desktop/Files/invoices_2012.csv CSV invoices_2012",
        b"C:/Users/barel/Desktop/Files/invoices_2013.csv CSV invoices_2013"
    ]

    producer.declare()
    producer.publish(b"C:/Users/barel/Desktop/Files/invoices_2012.csv CSV invoices_2012")
    producer.close()


if __name__ == '__main__':
    main()
