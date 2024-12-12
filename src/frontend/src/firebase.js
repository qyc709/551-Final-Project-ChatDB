// Import the functions you need from the SDKs you need
import { initializeApp } from "firebase/app";
import { getFirestore } from "firebase/firestore";

// TODO: Add SDKs for Firebase products that you want to use
// https://firebase.google.com/docs/web/setup#available-libraries

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyAOOdku2Naw1Lo2giBUc8kKNVhYzMV0mH8",
  authDomain: "dsci551-project-c3ce9.firebaseapp.com",
  projectId: "dsci551-project-c3ce9",
  storageBucket: "dsci551-project-c3ce9.appspot.com",
  messagingSenderId: "35248417232",
  appId: "1:35248417232:web:c13e3cbdfe6336615671d5",
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
