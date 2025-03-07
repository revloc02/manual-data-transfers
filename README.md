# Manual Data Transfers

This project exists for the following reasons:

* To increase my Java prowess.
* To deepen my understanding of various message brokers, cloud technologies, and other data transfer
  tools. Specifically, JMS, Qpid, AWS SQS, AWS SNS, AWS S3, and Azure Service Bus.
* To occasionally help me manually move data around between these various systems as a part of my
  work. Mind you, manual data transfers are the exception, and I only do them when absolutely
  necessary. But since my job is system integration these situations come up regularly.

### Reference:

I had a challenge getting my computer to push to both of my github accounts, and this page helped me
sort it: https://gist.github.com/Jonalogy/54091c98946cfe4f8cdab2bea79430f9

The key was the part that is setting up my `.ssh/config` file as an example here's what it currently
looks like:

```bash
#github main account
Host github.com
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_rsa
  IdentitiesOnly yes

#github personal account
Host github.com-revloc02
  HostName github.com
  User git
  IdentityFile ~/.ssh/id_rsa_xxxx_xxxxxx
  IdentitiesOnly yes
```

And then for my personal projects see the `.git/config` file as an example, specifically the
`remote` section:

```bash
[remote "origin"]
	url = git@github.com-revloc02:revloc02/manualDataTransfers.git
	fetch = +refs/heads/*:refs/remotes/origin/*
```
