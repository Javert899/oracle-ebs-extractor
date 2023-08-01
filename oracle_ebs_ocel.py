import pandas as pd
import pm4py
from datetime import datetime


def find_changes(dataframe, main_col, target_col, min_lim=2):
    main_cols_vals = set(dataframe[main_col].unique())
    stream = dataframe.to_dict("records")
    excluded = {"LATEST_EXTERNAL_FLAG", "APPROVED_DATE", "LAST_UPDATED_BY", "REVISION_NUM", "REVISED_DATE",
                "SUBMIT_DATE", "WF_ITEM_KEY", "LAST_UPDATE_LOGIN"}
    stream = [{x: str(y) for x, y in z.items() if x not in excluded} for z in stream]
    dct = {str(x): list() for x in main_cols_vals}
    ret = {str(x): list() for x in main_cols_vals}
    for v in stream:
        dct[v[main_col]].append(v)
    for x in dct:
        lst = dct[x]
        i = 1
        while i < len(lst):
            prev = lst[i - 1]
            curr = lst[i]
            chngs = set()
            for k, v in prev.items():
                if k != "LAST_UPDATE_DATE":
                    if v != curr[k]:
                        chngs.add((k, v, curr[k]))
            if chngs:
                chngs = sorted(list(chngs), key=lambda x: x[0])
                ret[x].append((lst[i]["LAST_UPDATE_DATE"], chngs))
            i = i + 1
    stream = []
    for x in ret:
        for y in ret[x]:
            dct = {target_col: int(x)}
            dct["time:timestamp"] = y[0]
            dct["concept:name"] = "Changed "+"+".join([z[0] for z in y[1]])
            stream.append(dct)
    dataframe = pd.DataFrame(stream)
    dataframe["time:timestamp"] = pd.to_datetime(dataframe["time:timestamp"])
    acti_count = dataframe["concept:name"].value_counts().to_dict()
    acti_count = {x: y for x, y in acti_count.items() if y >= min_lim}
    dataframe = dataframe[dataframe["concept:name"].isin(acti_count)]

    return ret, dataframe


def pay_sched_processing(payment_schedules, field):
    dataframe = payment_schedules.dropna(subset=[field])
    dataframe[field] = pd.to_datetime(dataframe[field], errors="coerce")
    dataframe = dataframe.dropna(subset=[field])
    dataframe["time:timestamp"] = dataframe[field] + pd.Timedelta(hours=23, minutes=59)
    dataframe["concept:name"] = "INVOICE " + field
    dataframe["INVOICE_OBJS"] = "INVOICE_" + dataframe["INVOICE_ID"].astype('string')
    return dataframe


goods_receipt = pd.read_csv("RCV_TRANSACTIONS.csv")
goods_receipt = goods_receipt[goods_receipt["TRANSACTION_TYPE"] == "RECEIVE"]
goods_receipt = goods_receipt.dropna(subset=["PO_HEADER_ID"])
goods_receipt["PO_HEADER_ID"] = goods_receipt["PO_HEADER_ID"].astype(int)
goods_receipt["ORDER_OBJS"] = "ORDER_" + goods_receipt["PO_HEADER_ID"].astype('string')
goods_receipt["time:timestamp"] = goods_receipt["CREATION_DATE"]
goods_receipt["concept:name"] = "GOODS RECEIPT"

gr_red = goods_receipt.dropna(subset=["REQUISITION_LINE_ID"])
gr_red["REQUISITION_LINE_ID"] = gr_red["REQUISITION_LINE_ID"].astype(int)
map_prl_po = gr_red[["PO_HEADER_ID", "REQUISITION_LINE_ID"]].to_dict("records")
map_prl_po = {x["REQUISITION_LINE_ID"]: x["PO_HEADER_ID"] for x in map_prl_po}

payment_schedules = pd.read_csv("AP_PAYMENT_SCHEDULES_ALL.csv")
payment_schedules["INVOICE_ID"] = payment_schedules["INVOICE_ID"].astype(int)
due_date = pay_sched_processing(payment_schedules, "DUE_DATE")
discount_date = pay_sched_processing(payment_schedules, "DISCOUNT_DATE")
second_discount_date = pay_sched_processing(payment_schedules, "SECOND_DISCOUNT_DATE")
third_discount_date = pay_sched_processing(payment_schedules, "THIRD_DISCOUNT_DATE")

reversals = pd.read_csv("AP_INVOICE_PAYMENTS_ALL.csv")
reversals = reversals.dropna(subset=["REVERSAL_INV_PMT_ID"])
reversals["INVPAY_OBJS"] = "INVPAY_" + reversals["REVERSAL_INV_PMT_ID"].astype(int).astype('string') + " AND INVPAY_" + reversals["INVOICE_PAYMENT_ID"].astype('string')
reversals["concept:name"] = "INVOICE REVERSED"
reversals["time:timestamp"] = reversals["LAST_UPDATE_DATE"]

ap_invoice_lines_all = pd.read_csv("AP_INVOICE_LINES_ALL.csv")
ap_invoice_lines_all = ap_invoice_lines_all.dropna(subset=["PO_HEADER_ID"])
ap_invoice_lines_all["PO_HEADER_ID"] = ap_invoice_lines_all["PO_HEADER_ID"].astype(int)
ap_invoice_lines_all["INVOICE_ID"] = ap_invoice_lines_all["INVOICE_ID"].astype(int)
pos = set(int(x) for x in ap_invoice_lines_all["PO_HEADER_ID"].unique())
po_action_history = pd.read_csv("PO_ACTION_HISTORY.csv")
po_action_history["OBJECT_ID"] = po_action_history["OBJECT_ID"].astype(int)
po_action_history = po_action_history[po_action_history["OBJECT_ID"].isin(pos)]
po_action_history["concept:name"] = po_action_history["OBJECT_TYPE_CODE"]+" "+po_action_history["ACTION_CODE"]
po_action_history["time:timestamp"] = po_action_history["CREATION_DATE"]

po_headers_archive_all = pd.read_csv("PO_HEADERS_ARCHIVE_ALL.csv")
ret_changes, po_headers_archive_all = find_changes(po_headers_archive_all, "PO_HEADER_ID", "OBJECT_ID")
ret_changes_lst = []
for x in ret_changes:
    for y in ret_changes[x]:
        date = y[0]
        chng_fields = y[1]
        for z in chng_fields:
            field = z[0]
            old_value = z[1]
            new_value = z[2]
            ret_changes_lst.append({"ocel:oid": "ORDER_"+x, "ocel:type": "ORDER_OBJS", "ocel:timestamp": datetime.fromisoformat(y[0]), "ocel:field": field, field: new_value})
ret_changes_lst = pd.DataFrame(ret_changes_lst)
print(ret_changes_lst)

po_action_history = pd.concat([po_action_history, po_headers_archive_all])

po_action_history["ORDER_OBJS"] = "ORDER_"+po_action_history["OBJECT_ID"].astype(str)

po_headers_all = pd.read_csv("PO_HEADERS_ALL.csv")
po_headers_all = po_headers_all.dropna(subset=["PO_HEADER_ID", "VENDOR_ID"], how="any")
po_headers_all["PO_HEADER_ID"] = po_headers_all["PO_HEADER_ID"].astype(int)
po_headers_all["VENDOR_ID"] = po_headers_all["VENDOR_ID"].astype(int)
po_to_vendor = po_headers_all.groupby("PO_HEADER_ID")["VENDOR_ID"].agg(set)
po_to_vendor = {"ORDER_"+str(x): " AND ".join(sorted(["VENDOR_"+str(z) for z in y])) for x, y in po_to_vendor.items()}
po_action_history["VENDOR_OBJS"] = po_action_history["ORDER_OBJS"].map(po_to_vendor)

distributions = pd.read_csv("PO_DISTRIBUTIONS_ALL.csv")
distributions["concept:name"] = "ORDER DISTRIBUTION"
distributions["time:timestamp"] = distributions["LAST_UPDATE_DATE"]
distributions["PO_DISTRIBUTION_ID"] = distributions["PO_DISTRIBUTION_ID"].astype(int)
distributions["PO_HEADER_ID"] = distributions["PO_HEADER_ID"].astype(int)
map_distr_po = distributions.groupby("PO_DISTRIBUTION_ID")["PO_HEADER_ID"].first().to_dict()

"""po_distributions_archive_all = pd.read_csv("PO_DISTRIBUTIONS_ARCHIVE_ALL.csv")
po_distributions_archive_all = find_changes(po_distributions_archive_all, "PO_DISTRIBUTION_ID", "PO_DISTRIBUTION_ID")
po_distributions_archive_all["PO_HEADER_ID"] = po_distributions_archive_all["PO_DISTRIBUTION_ID"].map(map_distr_po)
distributions = pd.concat([distributions, po_distributions_archive_all])

distributions["ORDER_OBJS"] = "ORDER_" + distributions["PO_HEADER_ID"].astype(str)
distributions["DISTR_OBJS"] = "DISTR_" + distributions["PO_DISTRIBUTION_ID"].astype(str)"""

distributions = distributions[["REQ_DISTRIBUTION_ID", "PO_HEADER_ID"]].dropna(subset=["REQ_DISTRIBUTION_ID"])
distributions["REQ_DISTRIBUTION_ID"] = distributions["REQ_DISTRIBUTION_ID"].astype(int)

po_req_distributions_all = pd.read_csv("PO_REQ_DISTRIBUTIONS_ALL.csv")
po_req_distributions_all["DISTRIBUTION_ID"] = po_req_distributions_all["DISTRIBUTION_ID"].astype(int)
po_req_distributions_all["REQUISITION_LINE_ID"] = po_req_distributions_all["REQUISITION_LINE_ID"].astype(int)

po_req_lines_all = pd.read_csv("PO_REQUISITION_LINES_ALL.csv")
po_req_lines_all["REQUISITION_LINE_ID"] = po_req_lines_all["REQUISITION_LINE_ID"].astype(int)
po_req_lines_all["REQUISITION_HEADER_ID"] = po_req_lines_all["REQUISITION_HEADER_ID"].astype(int)
po_req_lines_all = po_req_lines_all.merge(po_req_distributions_all, left_on="REQUISITION_LINE_ID", right_on="REQUISITION_LINE_ID")
po_req_lines_all = po_req_lines_all.merge(distributions, left_on="DISTRIBUTION_ID", right_on="REQ_DISTRIBUTION_ID")

po_req_headers_all = pd.read_csv("PO_REQUISITION_HEADERS_ALL.csv")
po_req_headers_all["REQUISITION_HEADER_ID"] = po_req_headers_all["REQUISITION_HEADER_ID"].astype(int)
po_req_headers_all = po_req_headers_all.merge(po_req_lines_all, left_on="REQUISITION_HEADER_ID", right_on="REQUISITION_HEADER_ID")
po_req_headers_all["PO_HEADER_ID"] = po_req_headers_all["PO_HEADER_ID"].astype(int)
po_req_headers_all["time:timestamp"] = po_req_headers_all["CREATION_DATE"]
po_req_headers_all["concept:name"] = "CREATE PURCHASE REQUISITION"
po_req_headers_all["ORDER_OBJS"] = "ORDER_" + po_req_headers_all["PO_HEADER_ID"].astype('string')
po_req_headers_all["REQ_OBJS"] = "REQ_" + po_req_headers_all["REQUISITION_HEADER_ID"].astype('string')

#po_req_headers_all = po_req_headers_all.merge(po_req_lines_all, left_on="REQUISITION_HEADER_ID", right_on="REQUISITION_HEADER_ID")
#po_req_headers_all["PO_HEADER_ID"] = po_req_headers_all["REQUISITION_LINE_ID"].map(map_prl_po)
#po_req_headers_all = po_req_headers_all.dropna(subset=["PO_HEADER_ID"])

invoices = pd.read_csv("AP_INVOICES_ALL.csv")
invoices["concept:name"] = "INVOICE CREATED"
invoices["time:timestamp"] = invoices["CREATION_DATE"]
invoices["INVOICE_ID"] = invoices["INVOICE_ID"].astype(int)
invoices["INVOICE_OBJS"] = "INVOICE_" + invoices["INVOICE_ID"].astype(str)
invoices["VENDOR_OBJS"] = "VENDOR_" + invoices["VENDOR_ID"].astype(str)

holds = pd.read_csv("AP_HOLDS_ALL.csv")
holds["INVOICE_ID"] = holds["INVOICE_ID"].astype(int)
holds["INVOICE_OBJS"] = "INVOICE_" + holds["INVOICE_ID"].astype(str)
releases = holds.dropna(subset=["RELEASE_LOOKUP_CODE"])
holds["time:timestamp"] = holds["HOLD_DATE"]
holds["concept:name"] = "INVOICE HELD"
releases["time:timestamp"] = releases["LAST_UPDATE_DATE"]
releases["concept:name"] = "INVOICE RELEASED"

ap_inv_aprvl_hist_all = pd.read_csv("AP_INV_APRVL_HIST_ALL.csv")
ap_inv_aprvl_hist_all["INVOICE_ID"] = ap_inv_aprvl_hist_all["INVOICE_ID"].astype(int)
ap_inv_aprvl_hist_all["INVOICE_OBJS"] = "INVOICE_" + ap_inv_aprvl_hist_all["INVOICE_ID"].astype(str)
ap_inv_aprvl_hist_all["concept:name"] = "INVOICE "+ap_inv_aprvl_hist_all["RESPONSE"]
ap_inv_aprvl_hist_all["time:timestamp"] = ap_inv_aprvl_hist_all["LAST_UPDATE_DATE"]

inv_orders = ap_invoice_lines_all.groupby("INVOICE_ID")["PO_HEADER_ID"].agg(set).to_dict()
inv_orders = {x: " AND ".join(["ORDER_"+str(z) for z in y]) for x, y in inv_orders.items()}

invoices = pd.concat([invoices, holds, releases, ap_inv_aprvl_hist_all])
invoices["ORDER_OBJS"] = invoices["INVOICE_ID"].map(inv_orders)

invoice_payments_all = pd.read_csv("AP_INVOICE_PAYMENTS_ALL.csv")
invoice_payments_all = invoice_payments_all.groupby("INVOICE_PAYMENT_ID")["INVOICE_ID"].agg(set).to_dict()
payment_hist_dists = pd.read_csv("AP_PAYMENT_HIST_DISTS.csv").groupby("PAYMENT_HISTORY_ID")["INVOICE_PAYMENT_ID"].agg(set).to_dict()
payment_hist_dists_complete = {}
for pay in payment_hist_dists:
    inv_set = set()
    for inv_pay in payment_hist_dists[pay]:
        if inv_pay in invoice_payments_all:
            for el in invoice_payments_all[inv_pay]:
                inv_set.add(el)
    inv_set = {"INVOICE_"+str(x) for x in inv_set}
    inv_set = " AND ".join(inv_set)
    payment_hist_dists_complete[pay] = inv_set
payment_hist_dists = {x: " AND ".join(["INVPAY_"+str(z) for z in y]) for x, y in payment_hist_dists.items()}
payment_history_all = pd.read_csv("AP_PAYMENT_HISTORY_ALL.csv")
payment_history_all["concept:name"] = payment_history_all["TRANSACTION_TYPE"]
payment_history_all["time:timestamp"] = payment_history_all["CREATION_DATE"]
payment_history_all["PAYMENT_OBJS"] = "PAYMENT_"+payment_history_all["PAYMENT_HISTORY_ID"].astype(str)
payment_history_all["INVOICE_OBJS"] = payment_history_all["PAYMENT_HISTORY_ID"].map(payment_hist_dists_complete)
payment_history_all["INVPAY_OBJS"] = payment_history_all["PAYMENT_HISTORY_ID"].map(payment_hist_dists)

dataframe = pd.concat([po_action_history, invoices, payment_history_all, reversals, due_date, discount_date, second_discount_date, third_discount_date, goods_receipt, po_req_headers_all])
print(dataframe["concept:name"].value_counts())
dataframe = dataframe.dropna(subset=["concept:name", "time:timestamp"], how="any")
dataframe["time:timestamp"] = pd.to_datetime(dataframe["time:timestamp"])
dataframe = dataframe.sort_values("time:timestamp")
print(dataframe["concept:name"].value_counts())
ocel = pm4py.convert_log_to_ocel(dataframe, object_types=["ORDER_OBJS", "INVOICE_OBJS", "PAYMENT_OBJS", "INVPAY_OBJS", "VENDOR_OBJS", "REQ_OBJS"])
print("conversion done")
ocel.object_changes = ret_changes_lst
#pm4py.write_ocel2(ocel, "normal.xmlocel")
#pm4py.write_ocel2(ocel, "normal.sqlite")
if False:
    ocel = pm4py.ocel_e2o_lifecycle_enrichment(ocel)
    print("enriched E2O")
    ocel = pm4py.ocel_o2o_enrichment(ocel, ["object_interaction_graph"])
    print("enriched O2O")
pm4py.write_ocel2(ocel, "extended.sqlite")
