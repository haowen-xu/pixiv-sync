## Pixiv Sync Tool

### Installation

If you have Python installed:

```bash
pip install git+https://github.com/haowen-xu/pixiv-sync.git
```


### Usage

First, create a YAML configuration file.
Here is an example: [example.yml](example.yml).

Next, login with your web browser (like Chrome), and copy the login token 
from the browser (it should be the value of the `PHPSESSID` cookie):

```bash
PixivSync set-token -C your-config.yml "your-token-value from web browser"
```

Finally, you can fetch Pixiv illustrations by:

```bash
PixivSync sync -C your-config.yml
```

Enjoy yourself!
