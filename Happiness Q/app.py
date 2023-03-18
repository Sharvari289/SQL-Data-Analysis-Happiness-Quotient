from flask import Flask,render_template,url_for,request,flash,redirect
from firebase_commands import rm,cat,struct,mkdir,ls,put,getPartitionloc,readPartition
from sql_commands import sql_struct,sql_mkdir,sql_ls,sql_put,sql_getPartitionloc,sql_readPartition,sql_cat,sql_rm
from PIL import Image,ImageDraw
from map_commands import whr_top_10,whr_low_10,whr_low_year,whr_high_year,whr_mean,whr_median,gdp_max,gdp_min,unemp_max,unemp_min,gdp_top_10,gdp_low_10,unemp_avg
import requests
import json
import io
app = Flask(__name__)
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False
@app.route("/")
def home_page():
    return render_template('home.html')
@app.route('/firebase',methods=['GET','POST'])
def firebase():
    op=""
    csv=False
    if request.method=="POST":
        command=request.form.get('comm')
        print(command)
        path=request.form.get('path')
        if command=="mkdir":
            #print('in')
            msg=mkdir.mkdir(path)
            if "error" in msg:
                flash(msg,category="danger")
            else:
                flash(msg,category="success")
            return redirect(url_for('firebase'))

        if command=='ls':
            ans=ls.ls(path)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('firebase'))
            else:
                ans=[ans[i: i+3] for i in range(0, len(ans), 3)]
                ans=['      '.join(i)+'\n' for i in ans]
                op=ans
            #print(op)
        if command=='put':
            k=request.form.get('part')
            file_name=request.form.get('name')
            ans=put.put(file_name,path,k)
            if "error" in ans:
                flash(ans,category="danger")
            else:
                flash(ans,category="success")
            return redirect(url_for('firebase'))
        
        if command=="get Partition Loc":
            ans=getPartitionloc.getPartitionloc(path)
            print(ans)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('firebase'))
            else:
                ans=[ans[i: i+3] for i in range(0, len(ans), 3)]
                ans=['      '.join(i)+'\n' for i in ans]
                op=ans
            #print(op)

        if command =="read Partition":
            k=request.form.get('part')
            ans=readPartition.readPartition(path,k)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('firebase'))
            else:
                op=ans.to_html()
                csv=True
                print(op)
        
        if command=="cat":
            ans=cat.cat(path)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('firebase'))
            else:
                op=ans.to_html()
                csv=True
                #print(op)
        if command=="rm":
            ans=rm.rm(path)
            if "error" in ans:
                flash(ans,category="danger")
            else:
                flash(ans,category="success")
            return redirect(url_for('firebase'))




    tree=struct.struct()
    return render_template('firebase.html',t=tree,op=[op],csv=csv)

@app.route('/sql',methods=['GET','POST'])
def sql():
    op=""
    csv=False
    if request.method=="POST":
        command=request.form.get('comm')
        #print(command)
        path=request.form.get('path')
        if command=="mkdir":
            msg=sql_mkdir.mkdir(path)
            #print(msg)
            if "error" in msg:
                flash(msg,category="danger")
            else:
                flash(msg,category="success")
            return redirect(url_for('sql'))
        if command=="ls":
            ans=sql_ls.ls(path)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('sql'))
            else:
                ans=[ans[i: i+3] for i in range(0, len(ans), 3)]
                ans=['      '.join(i)+'\n' for i in ans]
                print(ans)
                op=ans

        if command=="put":
            k=request.form.get('part')
            file_name=request.form.get('name')
            ans=sql_put.put(file_name,path,k)
            if "error" in ans:
                flash(ans,category="danger")
            else:
                flash(ans,category="success")
            return redirect(url_for('sql'))

        if command=="get Partition Loc":
            ans=sql_getPartitionloc.getPartitionloc(path)
            print(ans)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('sql'))
            else:
                ans=[ans[i: i+3] for i in range(0, len(ans), 3)]
                ans=['      '.join(i)+'\n' for i in ans]
                op=ans

        if command =="read Partition":
            k=request.form.get('part')
            ans=sql_readPartition.readPartition(path,k)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('sql'))
            else:
                op=ans.to_html()
                csv=True
                #print(op)
            
        if command=="cat":
            ans=sql_cat.cat(path)
            if type(ans)==str:
                if "error" in ans:
                    flash(ans,category="danger")
                else:
                    flash(ans,category="success")
                return redirect(url_for('sql'))
            else:
                op=ans.to_html()
                csv=True
                #print(op)
        if command=="rm":
            ans=sql_rm.rm(path)
            if "error" in ans:
                flash(ans,category="danger")
            else:
                flash(ans,category="success")
            return redirect(url_for('sql'))

    tree=sql_struct.struct()
    #print(tree)
    return render_template('sql.html',t=tree,op=[op],csv=csv)

@app.route('/analytics',methods=['GET','POST'])
def analytics():
    whr_path='/users/pop2/pop3/whr.csv'
    gdp_path='/GDP.csv'
    unemp_path='/shar/shar1/unemployment-2020.csv'
    ans=""
    exp=""
    csv=False
    if request.method=="POST":
        dataset=request.form.get('dataset')
        analysis=request.form.get('analyze')
        print(dataset,analysis)
        if dataset=="whr" and analysis=="Top 10 for a specific year":
            csv=True
            year=int(request.form.get('year'))
            columns=request.form.get('columns')
            exp,ans=whr_top_10.top_10_main(whr_path,year,columns)
            
            ans=ans.to_html()
            print(ans)
        
        if dataset=="whr" and analysis=="Lowest 10 for a specific year":
            csv=True
            year=int(request.form.get('year'))
            columns=request.form.get('columns')
            exp,ans=whr_low_10.low_10_main(whr_path,year,columns)
            ans=ans.to_html()

        if dataset=="whr" and analysis=="Lowest factor score for a country":
            csv=True
            columns=request.form.get('columns')
            country=request.form.get('country')
            print(columns,country)
            exp,ans=whr_low_year.low_year_main(whr_path,columns,country)
            ans=ans.to_html()

        if dataset=="whr" and analysis=="Highest factor score for a country":
            csv=True
            columns=request.form.get('columns')
            country=request.form.get('country')
            print(columns,country)
            exp,ans=whr_high_year.high_year_main(whr_path,columns,country)
            ans=ans.to_html()
    
        if dataset=="whr" and analysis=="Mean factor score for a country":
            csv=True
            columns=request.form.get('columns')
            country=request.form.get('country')
            print(columns,country)
            exp,ans=whr_mean.mean_main(whr_path,columns,country)
            ans=ans.to_html()

        if dataset=="whr" and analysis=="Median factor score for a country":
            csv=True
            columns=request.form.get('columns')
            country=request.form.get('country')
            print(columns,country)
            exp,ans=whr_median.median_main(whr_path,columns,country)
            ans=ans.to_html()

    
        if dataset=="GDP" and analysis=="Maximum GDP":
            csv=True
            year=int(request.form.get('year'))
            type=request.form.get('type')
            print(year,type)
            exp,ans=gdp_max.max_main(gdp_path,year,type)
            ans=ans.to_html()
        
        if dataset=="GDP" and analysis=="Minimum GDP":
            csv=True
            year=int(request.form.get('year'))
            type=request.form.get('type')
            print(year,type)
            exp,ans=gdp_min.min_main(gdp_path,year,type)
            ans=ans.to_html()

        if dataset=="GDP" and analysis=="Top 10 GDP countries for a specific year":
            csv=True
            year=int(request.form.get('year'))
            type=request.form.get('type')
            print(year,type)
            exp,ans=gdp_top_10.max_main(gdp_path,year,type)
            ans=ans.to_html()

        if dataset=="GDP" and analysis=="Lowest 10 GDP countries for a specific year":
            csv=True
            year=int(request.form.get('year'))
            type=request.form.get('type')
            print(year,type)
            exp,ans=gdp_low_10.min_main(gdp_path,year,type)
            ans=ans.to_html()

        if dataset=="unemp" and analysis=="Maximum unemployment per month for a specific location and gender":
            csv=True
            gender=request.form.get('gender')
            country=request.form.get('Location')
            print(gender,country)
            exp,ans=unemp_max.max_main(unemp_path,country,gender)
            ans=ans.to_html()
            

        if dataset=="unemp" and analysis=="Minimum unemployment per month for a specific location and gender":
            csv=True
            gender=request.form.get('gender')
            country=request.form.get('Location')
            print(gender,country)
            exp,ans=unemp_min.min_main(unemp_path,country,gender)
            ans=ans.to_html()

        if dataset=="unemp" and analysis=="Average unemployment per month for a specific location and gender":
            csv=True
            gender=request.form.get('gender')
            country=request.form.get('Location')
            print(gender,country)
            exp,ans=unemp_avg.avg_main(unemp_path,country,gender)
            ans=ans.to_html()


    return render_template('analytics.html',ans=[ans],exp=exp,csv=csv)

if __name__=="__main__":
    app.run(debug=True)