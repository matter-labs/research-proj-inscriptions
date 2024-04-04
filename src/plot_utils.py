

import matplotlib
import plotly.graph_objects as go
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

colors = {'red': '#ee443a', 'blue': '#42bbf1', 'dark_blue': '#1a4fec',
          'green': '#50be61', 'grey': '#b7b7b7', 'orange': '#f28222', 'purple': '#6e18ee', 'brown': '#a65628', 'pink': '#ef4793',
          'yellow': '#f8c94c', 'black': '#000000', 'white': '#ffffff', 'light_blue': '#a6cee3', 'light_green': '#b2df8a',
          'light_grey': '#999999', 'light_orange': '#fdbf6f', 'light_purple': '#cab2d6', 'light_brown': '#ffff99', 'light_pink': '#1f78b4',
          'light_yellow': '#fb9a99', 'light_black': '#e31a1c', 'light_white': '#33a02c', 'gold': '#ff7f00', 'silver': '#b2df8a'}
styles = ['-', '--', ':', '-.']
percentiles = [.01, .05, .1, .2, .25, .50, .75, .8, .9, .95, .99]
linestyles = ['dotted', 'dotted', 'solid', 'dashdot', 'dashed', 'solid']


def get_plotly_layout(height, width):
    layout = go.Layout(
        template='simple_white',
        font=dict(size=18, family='Clear Sans'),
        margin=go.layout.Margin(
            l=10,  # left margin
            r=10,  # right margin
            b=10,  # bottom margin
            t=10  # top margin
        ),
        width=width,
        height=height,
        xaxis=dict(minor_ticks="inside", showgrid=True,
                   griddash='dash', minor_griddash="dot"),
        yaxis=dict(minor_ticks="inside", showgrid=True,
                   griddash='dash', minor_griddash="dot")
    )
    return layout
