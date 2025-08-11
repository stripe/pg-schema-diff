# Contributing

This project is in its early stages. We appreciate all the feature/bug requests we receive, but we have limited cycles 
to review direct code contributions at this time. We will try and respond to any bug reports, feature requests, and 
questions within one week.

## Set-up
1. Install Docker
2. [Setup GPG key signing](https://docs.github.com/en/authentication/managing-commit-signature-verification/adding-a-gpg-key-to-your-github-account)
3. *(Optional)* Install Postgres locally
4. *(Optional)* Install [golangci-lint (go linting)](https://github.com/golangci/golangci-lint)
5. *(Optional)* Install [sqlfluff (sql linting)](https://github.com/sqlfluff/sqlfluff)

If you want to make changes yourself, follow these steps:

1. [Fork](https://help.github.com/articles/fork-a-repo/) this repository and [clone](https://help.github.com/articles/cloning-a-repository/) it locally.
2. Make your changes
3. Test your changes
```bash
# builds image running tests and run the image
 docker build -t pg-schema-diff-test-runner -f ./build/Dockerfile.test && docker run pg-schema-diff-test-runner 
 ``` 
3. Submit a [pull request](https://help.github.com/articles/creating-a-pull-request-from-a-fork/)

## Contributor License Agreement ([CLA](https://en.wikipedia.org/wiki/Contributor_License_Agreement))

Once you have submitted a pull request, sign the CLA by clicking on the badge in the comment from [@CLAassistant](https://github.com/CLAassistant).

<img width="910" alt="image" src="https://user-images.githubusercontent.com/62121649/198740836-70aeb322-5755-49fc-af55-93c8e8a39058.png">

<br />
Thanks for contributing to Stripe! 
