import os

if __name__ == '__main__':
    # mr_cluster_id = os.popen('mrjob create-cluster --num-core-instances=3 --num-task-instances=0 --instance-type=m1.xlarge --pool-clusters --emr-action-on-failure=CONTINUE --release-label=emr-5.0.0 --max-mins-idle=30.0').read()
    # recommended_movie = os.popen(f'python RecommendMovieEMR.py u.data --items=u.item --runner=emr --cluster-id={mr_cluster_id} > RecommededMovie.txt').read()
    mr_cluster_id = os.popen('mrjob create-cluster --num-core-instances=3 --max-mins-idle=30.0').read()
    recommended_movie = os.popen(f'python RecommendMovieEMR.py u.data --items=u.item --runner=emr --cluster-id={mr_cluster_id} > RecommededMovie.txt').read()
    recommended_movie_txt = open("RecommededMovie.txt", "wt")
    recommended_movie_txt.write(recommended_movie)
    recommended_movie_txt.close()