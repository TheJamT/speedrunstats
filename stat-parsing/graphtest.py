import plotly.express as px

x = [1,2,3,4,5]
y = [1,3,4,5,6]

fig = px.line(x=x, y=y, title = "A simple line graph")

fig.write_image("fig1.png")