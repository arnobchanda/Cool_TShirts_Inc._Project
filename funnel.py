import pandas as pd

visits = pd.read_csv("visits.csv",parse_dates=[1])
cart = pd.read_csv('cart.csv',parse_dates=[1])
checkout = pd.read_csv('checkout.csv',parse_dates=[1])
purchase = pd.read_csv('purchase.csv',parse_dates=[1])

#print(visits.head(5))
#print(cart.head(5))
#print(checkout.head(5))
#print(purchase.head(5))

#-------------- Visit - Cart Funnel-----------
visit_cart_merge = pd.merge(visits,cart,how='left')
#print(visit_cart_merge.head(10))

#Merged Data Frame has 2052 rows and 3 columns

visit_cart_merge["Positive_cart_time"] = ~visit_cart_merge.cart_time.isnull()
#print(visit_cart_merge)
visit_cart_count = visit_cart_merge.groupby("Positive_cart_time").user_id.count().reset_index()
#print(visit_cart_count)
visit_cart_funnel = visit_cart_count.set_index("Positive_cart_time").T
visit_cart_funnel.rename(columns={False:"No_of_people_visiting_but_not_buying",True:"No_of_people_visiting_and_buying"},inplace=True)
#print(visit_cart_transposed)
visit_cart_funnel["Percentage_of_users_not_buying"] = visit_cart_funnel.No_of_people_visiting_but_not_buying / (visit_cart_funnel.No_of_people_visiting_but_not_buying+visit_cart_funnel.No_of_people_visiting_and_buying) * 100
print(visit_cart_funnel)
visit_cart_funnel.to_csv("Visit_Cart_Funnel.csv")

#---------------Cart - Checkout Funnel-----------

cart_checkout_merge = pd.merge(cart,checkout,how="left")
#print(cart_checkout_merge)

#Merged data has 603 rows and 3 columns

cart_checkout_merge["Positive_checkout_time"] = ~cart_checkout_merge.checkout_time.isnull()
cart_checkout_count = cart_checkout_merge.groupby("Positive_checkout_time").user_id.count().reset_index()
cart_checkout_funnel = cart_checkout_count.set_index("Positive_checkout_time").T
cart_checkout_funnel.rename(columns={False:"No_of_people_not_checking_out",True:"No_of_people_checking_out"},inplace=True)
cart_checkout_funnel["Percentage_of_users_not_checking_out"] = cart_checkout_funnel.No_of_people_not_checking_out  / (cart_checkout_funnel.No_of_people_not_checking_out + cart_checkout_funnel.No_of_people_checking_out) * 100
print(cart_checkout_funnel)
cart_checkout_funnel.to_csv("Cart_Checkout_Funnel.csv")

#----------------Checkout - Purchase Funnel-----------

checkout_purchase_merge = pd.merge(checkout,purchase,how="left")
#print(checkout_purchase_merge)

#Merged data has 598 rows and 3 columns

checkout_purchase_merge["Positive_purchase_time"] = ~checkout_purchase_merge.purchase_time.isnull()
checkout_purchase_count = checkout_purchase_merge.groupby("Positive_purchase_time").user_id.count().reset_index()
checkout_purchase_funnel = checkout_purchase_count.set_index("Positive_purchase_time").T
checkout_purchase_funnel.rename(columns={False:"No_of_people_not_purchasing",True:"No_of_people_purchasing"},inplace=True)
checkout_purchase_funnel["Percentage_of_users_not_purchasing"] = checkout_purchase_funnel.No_of_people_not_purchasing / (checkout_purchase_funnel.No_of_people_not_purchasing + checkout_purchase_funnel.No_of_people_purchasing) * 100
print(checkout_purchase_funnel)
checkout_purchase_funnel.to_csv("Checkout_Purchase_Funnel.csv")

#-----------------All the merged contents merged together--------------------
all_data  = visit_cart_merge.merge(cart_checkout_merge,how="left").merge(checkout_purchase_merge,how="left")
all_data['time_to_purchase'] = all_data.purchase_time - all_data.visit_time
#print(all_data)
all_data.to_csv("All_data_merged_together.csv")
each_user_time_to_purchase = all_data.time_to_purchase
#print(each_user_time_to_purchase)
each_user_time_to_purchase.to_csv("Each_user's_time_to_purchase.csv")
print(all_data.time_to_purchase.mean())