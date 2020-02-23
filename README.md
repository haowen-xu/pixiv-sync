## Pixiv Sync Tool

### Installation

If you have Python installed:

```bash
pip install git+https://github.com/haowen-xu/pixiv-sync.git
```


### Usage

First, create a YAML configuration file.
Here is an example: [example.yml](example.yml).
You may save it as `config.yml`.

Next, login with your web browser (like Chrome), and copy the login token 
from the browser.  It should be the value of the `PHPSESSID` cookie.
You may see a tutorial at [tutorial/get-pixiv-token.mp4](tutorial/get-pixiv-token.mp4).

Then, save the token by:

```bash
PixivSync set-token -C config.yml "your-token-value from web browser"
```

Finally, you can fetch Pixiv illustrations by:

```bash
PixivSync sync -C config.yml
```

Enjoy yourself!
