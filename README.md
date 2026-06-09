# polybot-sdk-v2

Get your Rust GUI running on Windows Server 2019 **without a GPU** by using **Mesa3D software OpenGL**.

---

## Step 1: Download Mesa3D Windows Build

1. Go to this GitHub page:
   [https://github.com/pal1000/mesa-dist-win](https://github.com/pal1000/mesa-dist-win)
2. Scroll to **Releases** and download the latest **`mesa-dist-win-<version>-x64.zip`** (choose x64 if your server is 64-bit, which it almost certainly is).

---

## Step 2: Extract the DLLs

1. Unzip the file anywhere convenient, e.g., `C:\mesa3d`.
2. Inside, you’ll find a folder `bin` with **`opengl32.dll`** (and maybe `glx.dll`, etc.).
3. Copy **`opengl32.dll`** into the **same folder as your Rust executable**.

   * Example:

     ```
     C:\polybot\release\polybot.exe
     C:\polybot\release\opengl32.dll   <-- copy here
     ```
3. Copy all other **`***.dll`** into the **same folder as your Rust executable**. (mostly dependencies)

> Windows will now use Mesa’s software OpenGL instead of the default Microsoft driver.

---

## Step 3: Run your Rust app

* Double-click your `polybot.exe` or run it from the command line.
* You should now bypass the **OpenGL 2.0+ error** and your GUI should start.

---

### ✅ Notes:

* **Performance:** Software OpenGL is slower than hardware GPU. Good for testing, dashboards, or headless servers, but not for heavy rendering.
* **Debugging:** Mesa will print warnings/errors to the console; these are usually safe to ignore.
* **No GPU needed:** This works even if your server has no graphics card installed.

---

### 1️⃣ Mesa ZINK Vulkan warning

```
MESA: error: ZINK: failed to load vulkan-1.dll
```

* **Cause:** Mesa3D tried to use the ZINK driver (OpenGL over Vulkan) but Vulkan isn’t installed on your server.
* **Effect:** Usually harmless. Mesa can fall back to its **classic OpenGL software renderer**, which is what you want.
* **Action:** You can ignore this unless you specifically want Vulkan acceleration. Mesa will still provide OpenGL 2.0+ in software mode.

---

### 2️⃣ Glow context warning

```
Failed to create context using default context attributes ... os error 203
```

* `os error 203` means **“The system could not find the environment option that was entered”**.
* This happens because **some attributes for the OpenGL context are optional**, and Mesa’s software context may not support them all.
* **Effect:** If your app still creates a window and GUI appears, this warning is safe to ignore.

---

---

### wgpu

Config.toml 
eframe = { version = "...", features = ["wgpu"] }

main.rs
    let native_options = eframe::NativeOptions {
        renderer: eframe::Renderer::Wgpu,
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1100.0, 750.0])
            .with_min_inner_size([800.0, 500.0]),
        ..Default::default()
    };