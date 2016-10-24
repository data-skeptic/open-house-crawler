aws configure set aws_access_key_id $AWS_ACCESS_KEY
aws configure set aws_secret_access_key $AWS_SECRET_KEY
aws configure set region us-east-1

export BUCKET_NAME=open-house-crawl-`echo $TLD`

export BUCKET_EXISTS=`aws s3 ls | grep $BUCKET_NAME | wc -l`

if [ $BUCKET_EXISTS = 0 ]; then
	echo "Creating S3 bucket"
	aws s3 mb s3://$BUCKET_NAME
fi

cd
wget https://github.com/data-skeptic/open-house-crawler/archive/master.zip
unzip master
cd open-house-crawler-master/
cd $TLD
sed -i -e 's/..\/util\/api_push/api_push/g' lambda_function.py 
zip ~/deps.zip *.py
cd ../util/
zip -r ~/deps.zip api_push.py

echo api_user=$api_user > api_creds.conf
echo api_passwd=$api_passwd >> api_creds.conf
echo api_baseurl=$api_baseurl >> api_creds.conf
zip -r ~/deps.zip api_creds.conf
rm -f api_creds.conf

cd /usr

for dir in lib64/python2.7/site-packages \
             lib/python2.7/site-packages
do
  if [ -d $dir ] ; then
    pushd $dir; zip -r ~/deps.zip .; popd
  fi
done

mkdir -p local/lib
cp /usr/lib64/liblapack.so.3 \
   /usr/lib64/libblas.so.3 \
   /usr/lib64/libgfortran.so.3 \
   /usr/lib64/libquadmath.so.0 \
   local/lib/
zip -r ~/deps.zip local/lib

cd

# make an issolated directory
cd
mkdir ohtmp
mv deps.zip ohtmp/
cd ohtmp
unzip deps.zip
rm -f deps.zip

find . -name "*.pyc" -exec rm '{}' ';'
find . -name "*.pyo" -exec rm '{}' ';'
find . -name "*.txt" -exec rm '{}' ';'
find . -name "*.exe" -exec rm '{}' ';'
rm -R -f ./pandas/io/tests/
rm -R -f ./PIL
rm -R -f ./rsa
rm -R -f ./yum
rm -R -f ./kitchen
rm -R -f ./zope
rm -R -f ./awscli
rm -R -f ./pip
rm -R -f ./docutils

zip -r ~/deps.zip .

cd

export FN_NAME=OH-`python -c "print('$TLD'.replace('.', '-'))"`

export EXISTS=`aws lambda list-functions | grep $FN_NAME | wc -l`

aws s3 cp deps.zip s3://my-lambda-deployments/

if [ $EXISTS = 0 ]; then
	echo "Creating function"
	aws lambda create-function \
		--function-name $FN_NAME \
		--code S3Bucket=my-lambda-deployments,S3Key=deps.zip \
		--handler lambda_function.handler \
		--runtime python2.7 \
		--timeout 30 \
		--role arn:aws:iam::085318171245:role/service-role/OpenHouseParser \
		--memory-size 512 > result.json

	export FN_ARN=`python -c "import json; o = json.load(open('result.json')); print(o['FunctionArn'])"`

	aws lambda add-permission \
		--function-name $FN_NAME \
		--statement-id `uuidgen` \
		--action "lambda:InvokeFunction" \
		--principal s3.amazonaws.com \
		--source-arn arn:aws:s3:::$BUCKET_NAME

	echo "{ \
		\"LambdaFunctionConfigurations\": [ \
			{ \
				\"LambdaFunctionArn\": \"`echo $FN_ARN`\", \
				\"Events\": [\"s3:ObjectCreated:*\"], \
				\"Id\": \"3ab23772-3626-45fb-beec-4ccb454eb231\", \
				\"Filter\": { \
					\"Key\": { \
						\"FilterRules\": [ \
							{\"Name\": \"prefix\", \"Value\": \"everyhome.com\"}, \
							{\"Name\": \"suffix\", \"Value\": \"content.json\"} \
						] \
					} \
				} \
			} \
		] \
	}" > notification.json

	aws s3api put-bucket-notification-configuration --bucket $BUCKET_NAME --notification-configuration file://notification.json
else
	echo "Updating function"
	aws lambda update-function-code \
	--function-name $FN_NAME \
	--s3-bucket my-lambda-deployments \
    --s3-key deps.zip

fi

