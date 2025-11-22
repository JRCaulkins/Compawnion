git pull
python update_parks.py
git add .
git commit -m "Automatic website update"
git push -f origin HEAD:gh-pages
git push -f origin HEAD:main
