# Manual Data Transfers

This project is Java code for me to manually move data around, whether that be sending messages to queues (Qpid, SQS), accessing AWS S3s, or anything like that. It can connect to both on-prem and cloud solutions and transfer data to and from them, or even between them.

### Reference:

I had a challenge getting my computer to push to both of my github accounts, and this page helped me sort it: https://gist.github.com/Jonalogy/54091c98946cfe4f8cdab2bea79430f9 

The key was the part that is setting up my `.ssh/config` file as an example here's what it currently looks like:
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

And then for my personal projects see the `.git/config` file as an example, specifically the `remote` section:
```bash
[remote "origin"]
	url = git@github.com-revloc02:revloc02/manualDataTransfers.git
	fetch = +refs/heads/*:refs/remotes/origin/*
```
