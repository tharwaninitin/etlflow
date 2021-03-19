# Updating and testing docs

sbt -v "project docs" makeMicrosite

cd modules/docs/target/site

jekyll serve -b /etlflow/site

# Deploying 

rm -r modules/docs/target/site/_site modules/docs/target/site/.jekyll-cache

rm -r site

cp -R modules/docs/target/site site/