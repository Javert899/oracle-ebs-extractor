import sapextractor

c = sapextractor.connect_oracle("host", "port", "SID", "user", "password")

if True:
    cols = ["OBJECT_ID", "OBJECT_TYPE_CODE", "LAST_UPDATE_DATE", "LAST_UPDATED_BY", "CREATION_DATE", "CREATED_BY", "ACTION_CODE", "ACTION_DATE", "EMPLOYEE_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM po.PO_ACTION_HISTORY", cols)
    df.to_csv("PO_ACTION_HISTORY.csv", index=False)

if True:
    cols = ["PO_HEADER_ID", "FROM_HEADER_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM po.PO_LINES_ALL WHERE FROM_HEADER_ID is not null", cols)
    df.to_csv("PO_LINES_ALL.csv", index=False)

if True:
    cols = ["PO_DISTRIBUTION_ID", "PO_HEADER_ID", "LAST_UPDATE_DATE", "CREATION_DATE", "CREATED_BY", "REQ_DISTRIBUTION_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM po.PO_DISTRIBUTIONS_ALL", cols)
    df.to_csv("PO_DISTRIBUTIONS_ALL.csv", index=False)

if True:
    cols = ["DISTRIBUTION_ID", "REQUISITION_LINE_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM po.PO_REQ_DISTRIBUTIONS_ALL", cols)
    df.to_csv("PO_REQ_DISTRIBUTIONS_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "PO_HEADER_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_INVOICE_LINES_ALL", cols)
    df.to_csv("AP_INVOICE_LINES_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "INVOICE_PAYMENT_ID", "LAST_UPDATED_BY", "LAST_UPDATE_DATE", "CREATED_BY", "CREATION_DATE", "REVERSAL_FLAG", "REVERSAL_INV_PMT_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_INVOICE_PAYMENTS_ALL", cols)
    df.to_csv("AP_INVOICE_PAYMENTS_ALL.csv", index=False)

if True:
    cols = ["PAYMENT_HISTORY_ID", "INVOICE_PAYMENT_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_PAYMENT_HIST_DISTS WHERE INVOICE_PAYMENT_ID is not null", cols)
    df.to_csv("AP_PAYMENT_HIST_DISTS.csv", index=False)

if True:
    cols = ["PAYMENT_HISTORY_ID", "TRANSACTION_TYPE", "CREATION_DATE", "CREATED_BY", "LAST_UPDATE_DATE", "LAST_UPDATED_BY"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_PAYMENT_HISTORY_ALL", cols)
    df.to_csv("AP_PAYMENT_HISTORY_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "HOLD_DATE", "HOLD_LOOKUP_CODE", "LAST_UPDATE_DATE", "RELEASE_LOOKUP_CODE", "STATUS_FLAG"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_HOLDS_ALL", cols)
    df.to_csv("AP_HOLDS_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "CREATION_DATE", "LAST_UPDATE_DATE", "ITERATION", "RESPONSE"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_INV_APRVL_HIST_ALL", cols)
    df.to_csv("AP_INV_APRVL_HIST_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "INVOICE_DATE", "INVOICE_AMOUNT", "LAST_UPDATE_DATE", "CREATION_DATE", "VENDOR_ID"]
    df = c.execute_read_sql("SELECT "+", ".join(cols)+" FROM ap.AP_INVOICES_ALL", cols)
    df.to_csv("AP_INVOICES_ALL.csv", index=False)

if True:
    cols = ["PO_HEADER_ID", "VENDOR_ID"]
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.PO_HEADERS_ALL", cols)
    df.to_csv("PO_HEADERS_ALL.csv", index=False)

if True:
    cols = ["COLUMN_NAME"]
    df = c.execute_read_sql("SELECT COLUMN_NAME FROM sys.all_tab_columns WHERE TABLE_NAME = 'PO_HEADERS_ARCHIVE_ALL'", cols)
    cols = list(df["COLUMN_NAME"])
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.PO_HEADERS_ARCHIVE_ALL WHERE PO_HEADER_ID IN (SELECT PO_HEADER_ID FROM po.PO_HEADERS_ARCHIVE_ALL WHERE REVISION_NUM >= 1)", cols)
    df.to_csv("PO_HEADERS_ARCHIVE_ALL.csv", index=False)

if True:
    cols = ["COLUMN_NAME"]
    df = c.execute_read_sql("SELECT COLUMN_NAME FROM sys.all_tab_columns WHERE TABLE_NAME = 'PO_DISTRIBUTIONS_ARCHIVE_ALL'", cols)
    cols = list(df["COLUMN_NAME"])
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.PO_DISTRIBUTIONS_ARCHIVE_ALL WHERE PO_DISTRIBUTION_ID IN (SELECT PO_DISTRIBUTION_ID FROM po.PO_DISTRIBUTIONS_ARCHIVE_ALL WHERE REVISION_NUM >= 1)", cols)
    df.to_csv("PO_DISTRIBUTIONS_ARCHIVE_ALL.csv", index=False)

if True:
    cols = ["INVOICE_ID", "DUE_DATE", "DISCOUNT_DATE", "SECOND_DISCOUNT_DATE", "THIRD_DISCOUNT_DATE"]
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM ap.AP_PAYMENT_SCHEDULES_ALL", cols)
    df.to_csv("AP_PAYMENT_SCHEDULES_ALL.csv", index=False)

if True:
    cols = ["CREATION_DATE", "TRANSACTION_TYPE", "TRANSACTION_DATE", "PO_HEADER_ID", "REQUISITION_LINE_ID"]
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.RCV_TRANSACTIONS", cols)
    df.to_csv("RCV_TRANSACTIONS.csv", index=False)

if True:
    cols = ["REQUISITION_HEADER_ID", "CREATION_DATE"]
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.PO_REQUISITION_HEADERS_ALL", cols)
    df.to_csv("PO_REQUISITION_HEADERS_ALL.csv", index=False)

if True:
    cols = ["REQUISITION_HEADER_ID", "REQUISITION_LINE_ID"]
    df = c.execute_read_sql("SELECT " + ", ".join(cols) + " FROM po.PO_REQUISITION_LINES_ALL", cols)
    df.to_csv("PO_REQUISITION_LINES_ALL.csv", index=False)
