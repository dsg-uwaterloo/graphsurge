# Experiments

## Prepare
```bash
#!/bin/bash

set -euo pipefail; shopt -s nullglob

root=$HOME/graphsurge/experiments

# Download Graphsurge
git clone https://github.com/dsg-uwaterloo/graphsurge $HOME/graphsurge

# Build Graphsurge
cd $root/docker
cat Dockerfile.sample | sed -i -e "s/UID/$(id -u)/g" -e "s/GID/$(id -g)/g" > Dockerfile
docker build -t graphsurge .
mkdir $root/bin
cd $root/..
bash $root/build_gs_exe.sh

# Runtime configuration
runtypediffs=adaptivealldiffs
runtypediffs2=adaptivealldiffs2
runtyperestart=adaptiveallrestart
adaptive=adaptive
```

## 6.2
```bash
current_root=$root/6.2

## Prepare raw dataset
mkdir -p ${current_root}/graph/serde
cd ${current_root}/graph
[[ -f edges.txt ]] ||  {
    wget https://snap.stanford.edu/data/sx-stackoverflow.txt.gz;
    gunzip sx-stackoverflow.txt.gz;
    ln -snf sx-stackoverflow.txt edges.txt;
    # Add required headers
    sed -i "1i :start_id :end_id ts:int" edges.txt;
}
cd ${current_root}

## Load graph into Graphsurge
bash $root/run_docker.sh /opt/graphsurge/experiments/generate_graph_edges_only_space.sh

for i in 1_30  2_18  3_7  4_6  5_4  6_16  7_9  8_5  9_3 10_3 ;do
    cd $i
    mkdir -p vcol_data/{vcol_best_order_serde,vcol_manual_order_serde}
    
    for order in manual ;do
        echo $order

        ## Create view collections
        bash $root/run_docker.sh /opt/graphsurge/experiments/bin/gs create_vcol_queries/vcol_${order}_order.txt | tee vcol_${order}_order.log

        ## Run computations
        for computation in wcc bfs sssp mpsp pr scc ;do
            echo $computation
            for runtype in $runtypediffs $runtyperestart $adaptive ;do
                echo $runtype
                $root/run_docker.sh ../run_computation.sh $computation $runtype $order || break 3
            done
        done
    done
    cd ..
done
```

## 6.3
```bash
current_root=$root/6.3

## Prepare raw dataset
mkdir -p ${current_root}/graph/serde
cd ${current_root}/graph
[[ -f edges.txt && -f vertices.txt ]] ||  {
    mkdir raw
    cd raw
    wget https://s3-us-west-2.amazonaws.com/ai2-s2-research-public/open-corpus/2019-10-01/manifest.txt
    wget -B https://s3-us-west-2.amazonaws.com/ai2-s2-research-public/open-corpus/2019-10-01/ -i manifest.txt
    cd ..
    python $root/scripts/semanticscholar/generate_raw.py ${current_root}/graph/raw
    python $root/scripts/semanticscholar/generate_write/py
    ln -snf paper.csv vertices.txt
    ln -snf cite.csv edges.txt
    # Add required headers
    sed -i "1i id,year:int,authors:int" vertices.txt
    sed -i "1i src,dst" edges.txt
}
cd ${current_root}

## Load graph into Graphsurge
bash $root/run_docker.sh /opt/graphsurge/experiments/generate_graph_edges_vertices_comma

for i in 1_16_0 2_16_0 3_25_253 ;do
    cd $i
    
    mkdir -p vcol_data/{vcol_best_order_serde,vcol_manual_order_serde}
    
    for order in manual ;do
        echo $order

        ## Create view collections
        bash $root/run_docker.sh /opt/graphsurge/experiments/bin/gs create_vcol_queries/vcol_${order}_order.txt | tee vcol_${order}_order.log

        ## Run computations
        for computation in wcc bfs sssp mpsp pr scc ;do
            echo $computation
            for runtype in $runtypediffs $runtyperestart $adaptive ;do
                echo $runtype
                $root/run_docker.sh ../run_computation.sh $computation $runtype $order || break 3
            done
        done
    done
    cd ..
done
```

# 6.4
```bash
for dataset in comlj wikitopcats;do
    current_root=$root/6.4/$dataset

    ## Prepare raw dataset
    mkdir -p ${current_root}/graph/serde
    cd ${current_root}/graph
    [[ -f edges.txt && -f vertices.txt ]] ||  {
        if [[ $dataset == "comlj" ]];then
            filename=com-lj.ungraph.txt
            wget https://snap.stanford.edu/data/bigdata/communities/${filename}.gz
            gunzip ${filename}.gz;
            ln -snf ${filename} edges.txt

            filename2=com-lj.all.cmty.txt
            wget https://snap.stanford.edu/data/bigdata/communities/${filename2}.gz
            gunzip ${filename2}.gz
            ln -snf ${filename2} communities.txt
        else
            filename=wiki-topcats.txt
            wget https://snap.stanford.edu/data/${filename}.gz
            gunzip ${filename}.gz;
            ln -snf ${filename} edges.txt

            filename2=wiki-topcats-categories.txt
            wget https://snap.stanford.edu/data/${filename2}.gz
            gunzip ${filename2}.gz
            ln -snf ${filename2} communities.txt
        fi
        python $root/scripts/generate_community_graph.py ${dataset}
    }
    cd ${current_root}

    ## Load graph into Graphsurge
    bash $root/run_docker.sh /opt/graphsurge/experiments/generate_graph_edges_vertices_comma

    ### One ordered view collection
    for i in comm_10_5 comm_7_4 ;do
        cd $i
        
        mkdir -p vcol_data/{vcol_best_order_serde,vcol_manual_order_serde}
        
        for order in best ;do
            echo $order

            ## Create view collections
            bash $root/run_docker.sh /opt/graphsurge/experiments/bin/gs create_vcol_queries/vcol_${order}_order.txt | tee vcol_${order}_order.log

            ## Run computations
            for computation in wcc bfs sssp mpsp pr scc ;do
                echo $computation
                for runtype in $runtypediffs $adaptive ;do
                    echo $runtype
                    $root/run_docker.sh ../run_computation.sh $computation $runtype $order || break 3
                done
            done
        done
        cd ..
    done
    ### Three random view collections
    for i in comm_10_5 comm2_10_5 comm3_10_5 comm_7_4 comm2_7_4 comm3_7_4 ;do
        cd $i
        for order in manual ;do
            echo $order

            ## Create view collections
            bash $root/run_docker.sh /opt/graphsurge/experiments/bin/gs create_vcol_queries/vcol_${order}_order.txt | tee vcol_${order}_order.log

            ## Run computations
            for computation in wcc bfs sssp mpsp pr scc ;do
                echo $computation
                for runtype in $runtypediffs $adaptive ;do
                    echo $runtype
                    $root/run_docker.sh ../run_computation.sh $computation $runtype $order || break 3
                done
            done
        done
        cd ..
    done

done
```

# 6.5
```bash
## Build graph-map
git clone https://github.com/frankmcsherry/graph-map.git $root/graphmap
cd graphmap
bash $root/run_docker.sh cargo build --release && cp target/release/parse $root/bin/

## Build graphbolt
git clone https://github.com/sigmod-2021-195/graphbolt.git $root/graphbolt
cd graphmap
# Follow README instructions to compile the Graphbolt apps and tools.

for dataset in soclj orkut twitter;do
    current_root=$root/6.5/$dataset

    ## Prepare raw dataset
    mkdir -p ${current_root}/graph/serde
    cd ${current_root}/graph
    [[ -f edges.txt ]] ||  {
        if [[ $dataset == "soclj" ]];then
            filename=soc-LiveJournal1.txt
            initial=34000000
            wget https://snap.stanford.edu/data/${filename}.gz;
            gunzip ${filename}.gz;
            ln -snf ${filename} edges.txt
            $root/bin/parse ${filename} ${dataset}.gmap
        elif [[ $dataset == "orkut" ]];then
            filename=com-orkut.ungraph.txt
            initial=58000000
            wget https://snap.stanford.edu/data/bigdata/communities/${filename}.gz
            gunzip ${filename}.gz;
            ln -snf ${filename} edges.txt
            $root/bin/parse ${filename} ${dataset}.gmap
        else
            filename=twitter_rv.net
            initial=700000000
            wget http://an.kaist.ac.kr/~haewoon/release/twitter_social_graph/twitter_rv.tar.gz
            tar xzf twitter_rv.tar.gz;
            ln -snf ${filename} edges.txt
            $root/bin/parse ${filename} ${dataset}.gmap
        fi
    }
    cd ${current_root}

    ## Create view collection
    for computation in sssp pr; do
        if [[ $computation == "sssp" ]];then
            i=5000
            comp=$runtypediffs2
            gb_computation=SSSP
        else
            i=1000
            comp=$runtypediffs
            gb_computation=PageRank
        fi

        ## Create view collection
        mkdir $i
        cd $i
        bash $root/6.5/create_vcol.sh 500 500 $initial $i${current_root}/graph/${dataset}.gmap

        ## Run computation: Graphsurge
        $root/run_docker.sh ../run_computation.sh $computation $comp
        ## Run computation: Graphbolt
        ../run_computation.sh gb $gb_computation $i
    done

done
```

# 6.6
```bash
current_root=$root/6.6

## Prepare raw dataset
mkdir -p ${current_root}/graph/serde
cd ${current_root}/graph
[[ -f edges.txt && -f vertices.txt ]] ||  {
    ln -s $root/6.5/graph/twitter/twitter_rv.net edges.txt
    python $root/scripts/generate_scalability_graph.py
}
cd ${current_root}

## Load graph into Graphsurge
bash $root/run_docker.sh /opt/graphsurge/experiments/generate_graph_edges_vertices_comma

## Create view collection
bash $root/run_docker.sh /opt/graphsurge/experiments/bin/gs create_vcol_queries/vcol_manual_order.txt | tee vcol_manual_order.log

computation=wcc # Set to one of "wcc bfs sssp mpsp"
num_machines=1 # Set to one of "1 2 4 8 12"
machine_id=0 # Set to correct machine id ranging from 0 to num_machines-1
# Set IP values in $root/run_docker.sh

## Run computation on all machines
$root/run_docker.sh ../run_computation.sh $computation 2_stage_differential manual $num_machines $machine_id
```
