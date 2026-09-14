package az.naraz.app;

import android.app.Activity;
import android.os.Bundle;
import android.webkit.WebView;
import android.webkit.WebSettings;
import android.graphics.Color;
import android.view.Window;

public class MainActivity extends Activity {
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        requestWindowFeature(Window.FEATURE_NO_TITLE);
        getWindow().setStatusBarColor(Color.rgb(9,11,13));
        WebView view = new WebView(this);
        WebSettings s = view.getSettings();
        s.setJavaScriptEnabled(true);
        s.setDomStorageEnabled(true);
        s.setBuiltInZoomControls(false);
        view.setBackgroundColor(Color.rgb(9,11,13));
        view.loadUrl("file:///android_asset/index.html");
        setContentView(view);
    }
}
