# A sample of mobile application.

This is an experimental package for mobile app.

First, you need to install gomobile.
Follow the instruction on https://github.com/golang/go/wiki/Mobile to install the gomobile command.

For iOS, you can build a mobile.app by:

```
gomobile build -bundleid=<bundle-id-of-your-app> -target=ios .
```
  
For Android, you can build a apk by:

```
gomobile build -target=android .
```

When the device receives a message, color of the screen changes.
To run the application, at least you need to change the key and addresses for the application (which are hard-coded).
