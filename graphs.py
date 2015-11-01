from flask import Flask,render_template,request,redirect
from bokeh.plotting import figure
from bokeh.embed import components 
import pandas as pd

app_challenge = Flask(__name__)
app_challenge.vars={}

@app_challenge.route('/display_graph',methods=['GET','POST'])
def display_graph():
  if request.method == 'GET':
    return render_template('interface.html')
  else:
    app_challenge.vars['graph_nbr'] = request.form['graph_nbr']

    data = pd.read_csv('plots.csv')
    #plot the data with bokeh
    if request.form['graph_nbr'] == 1:
      titre = 'how do mexican restaurants fare in cities with different percentage of hispanic population?'
      xlabel = 'percentage hispanic pop'
      ylabel = 'normalized review count'
    else:
      titre = 'Is there a winner takes all effect when many similar restaurants coexist?'
      xlabel = 'number of competitors'
      ylabel = 'normallized spread of review countt'

    plot = figure(title='%s' %titre,
              x_axis_label='date',
              x_axis_type='datetime')

    if (request.form['graph_nbr'] == '1'):
      x = list(data.hisp)
      y = list(data['reviews'].div(data['pop'], axis='index'))
    else:
      x = list(data.n_competitors)
      y = list(data.spread_rating)

    plot.scatter(x,y,marker='asterisk')
    script, div = components(plot)
    return render_template('end_graph.html', script=script, div=div)

if __name__ == "__main__":
    app_challenge.run(port=33507,debug=True)
