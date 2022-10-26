import time
def top_sort_df(data_df, data_type = 'pct', rolling_window = 66, top_num = 5):
    if rolling_window == 'weekly':
        roll_window = 1
        last_date = data_df.index[-1]
        for date in data_df.index[-6:-1][::-1]:
            last_date_stamp = int(time.mktime(time.strptime(str(last_date)[:10], "%Y-%m-%d")))
            date_stamp = int(time.mktime(time.strptime(str(date)[:10], "%Y-%m-%d")))
            if last_date_stamp - date_stamp == 86400:
                last_date = date
                roll_window = roll_window + 1
        rolling_window = roll_window

    if data_type == 'pct':
        res_df = data_df.pct_change(rolling_window)
        if top_num > 0:
            top_list = res_df.iloc[-1].sort_values().dropna().iloc[-top_num:].index.tolist()
        else:
            top_list = res_df.iloc[-1].sort_values().dropna().iloc[:-top_num].index.tolist()
        res = data_df[top_list].iloc[-rolling_window:] / data_df[top_list].iloc[-rolling_window] -1
        return res
    elif data_type == 'volatility':
        res_df = data_df.pct_change().rolling(30).std()
        top_list = res_df.iloc[-30:].dropna(axis = 1).mean().sort_values().iloc[-top_num:].index.tolist()
        res = res_df[top_list].iloc[-rolling_window:]
        return res
    elif data_type == 'mean':
        res_df = data_df
        if top_num > 0:
            top_list = res_df.iloc[-rolling_window:].mean().loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[-top_num:].index.tolist()
        else:
            top_list = res_df.iloc[-rolling_window:].mean().loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[:-top_num].index.tolist()
        res = res_df[top_list].iloc[-rolling_window:]
        return res
    elif data_type == 'new':
        res_df = data_df
        if top_num > 0:
            top_list = res_df.iloc[-1].loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[-top_num:].index.tolist()
        else:
            top_list = res_df.iloc[-1].loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[:-top_num].index.tolist()
        res = res_df[top_list].iloc[-rolling_window:]
        return res
    elif data_type == 'week_mean':
        res_df = data_df
        if top_num > 0:
            top_list = res_df.iloc[-5:].mean().loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[-top_num:].index.tolist()
        else:
            top_list = res_df.iloc[-5:].mean().loc[
                           res_df.iloc[-rolling_window].dropna().index].sort_values().iloc[:-top_num].index.tolist()
        res = res_df[top_list].iloc[-rolling_window:]
        return res