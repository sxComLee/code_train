def CountSales(sale_records):
    """Calculate number of sales for each product id.
    Args:
        sales_records: list of SaleRecord,SaleRecord is a named tuple,
            eg {product_id:"1",timestamp:1553751167}
    Returns:
        dict of {product_id :num_of_sales}.
           eg:{"1":1."2":1}
    """
    sales_count = {}
    for record in sale_records:
        sales_count[record[product_id]] +=1
        return sales_count

def TopSellingItems(sale_records,k=10):
    """
    Calcalate the bast selling k products
    Args:
        sales_records: list of SaleRecord, SaleRocord is a named tuple,
            eg {product_id:"1",timestamp:1553751167}
    Returns:
        List of k product_id, sorted by num of sales
    """
    sales_count = CountSales(sale_records)
    return heapq.nlargest(k,sales_count,key=sales_count.get)

