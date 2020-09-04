sbt -v makeMicrosite

cp -R site target

cp -R modules/docs/target/site/docs target/site/

cd target/site

jekyll serve -b /etlflow/site