if [ -z $1 ]; then echo "No release version set"; exit; fi

echo "SNAPSHOT-versions removed and changes.xml updated?"
read

# make a copy of this script and execute everything below
# since during a switch branch it can happen that the script
# is not there or modified and cannot be executed
tail -n +12 ./release.sh > ./_release-copy.sh
sh ./_release-copy.sh $1 &
exit

# exit when any command fails
set -e

releaseVersion=$1
featureVersion=${releaseVersion:2:1}
nextFeatureVersion=$((featureVersion+1))
newSnapshotVersion=${releaseVersion:0:2}${nextFeatureVersion}.0-SNAPSHOT
echo "Release Version is: $releaseVersion"
echo "Next Development Version is: $newSnapshotVersion"

# make sure we do not commit any unwanted local changes
git stash

# update license header and commit changes if any
mvn license:update-file-header
if ! git diff-index --quiet HEAD --; then
    git add -u
    git commit -m "Update license header"
fi

# make sure master is up to date
git checkout master
git pull
git checkout develop

# release start
mvn jgitflow:release-start -DallowUntracked -DreleaseVersion=$1 -DdevelopmentVersion=$newSnapshotVersion

# finish release
mvn jgitflow:release-finish -DallowUntracked
git stash pop

# remove copy of script file
rm -- "$0"
