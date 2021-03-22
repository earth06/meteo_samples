def plot_twin_heatmap(dfsmall,targetid,save=False,outdir="",close=False,name="first",ninzu="",famtype="",figsize=(12,8)):
    timerange2019={"first":("2019-01-01","2019-06-30"),"second":("2019-07-01","2019-12-31"),"all":("2019-01-01","2019-12-31")}
    timerange2020={"first":("2020-01-01","2020-06-30"),"second":("2020-07-01","2020-12-31"),"all":("2020-01-01","2020-12-31")}
    #日付操作の準備
    begin1,end1=timerange2019[name]
    begin2,end2=timerange2020[name]
    range2019=pd.date_range(begin1,end1,freq="D")
    range2020=pd.date_range(begin2,end2,freq="D")
    dfsm=dfsmall[dfsmall["id"]==targetid].copy()
    if dfsm.empty:
        print("empty dataframe")
        return
    #日付が重複してるやつは除外
    if dfsm["EXAMINE_DATE"].value_counts().max()>1:
        print("skip id ",targetid)
        return
    dfsm.sort_values("EXAMINE_DATE",inplace=True)    
    dfsm.index=pd.to_datetime(dfsm["EXAMINE_DATE"],format="%Y%m%d")
    dfsm=times_fotmat(dfsm,targetid)
    dfsm.rename(columns=times_converter,inplace=True)
    df2019=dfsm[begin1:end1].reindex(range2019)
    df2020=dfsm[begin2:end2].reindex(range2020)
    #日付ラベルをformatする
    df2019.index=[t.strftime(format="%Y-%m-%d") for t in df2019.index]
    df2020.index=[t.strftime(format="%Y-%m-%d") for t in df2020.index]
    #描画
    vmax=auto_scaling(df2019[FTIMES],df2020[FTIMES])
    fig=plt.figure(figsize=figsize,dpi=110)
    ax=fig.add_subplot(2,1,1)
    ax.patch.set_facecolor('gray')
    ax2=fig.add_subplot(2,1,2)
    ax2.patch.set_facecolor("gray")
    
    sns.heatmap(df2019[FTIMES].transpose(),ax=ax,cmap="inferno",vmin=0,vmax=vmax,cbar_kws={"extend":"max"})
    sns.heatmap(df2020[FTIMES].transpose(),ax=ax2,cmap="inferno",vmin=0,vmax=vmax,cbar_kws={"extend":"max"})
    fig.subplots_adjust(hspace=0.4)
    ax.grid(linewidth=0.3, linestyle="--")
    ax2.grid(linewidth=0.3, linestyle="--")
    fig.suptitle(targetid)
    if save:
        fig.savefig(f"{outdir}/{targetid}_{ninzu}_{famtype}_{name}.png", bbox_inches="tight")
    if close:
        plt.close()
    print("end" + targetid)
    return 0