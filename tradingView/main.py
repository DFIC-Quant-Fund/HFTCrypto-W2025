import pandas as pd
from lightweight_charts import Chart, topbar
import json
from typing import Literal

class COLOR(str):  # This is likely a placeholder and does not need instantiating
    pass

if __name__ == '__main__':
    
    chart = Chart()  
    color: COLOR = "#171B26"  # valid color format
    tb = topbar.TopBar(chart)

    # Columns: time | open | high | low | close | volume 
    df = pd.read_json('data.json')
    chart.set(df)
    chart.layout(background_color=color)

    chart.topbar.textbox('symbol', 'AAPL') # Declares a textbox displaying 'AAPL'.
    # chart.price_scale(auto_scale=False)
    
    chart.show(block=True)
