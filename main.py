# เพิ่มใน Python server (main.py หรือ app.py)
from flask import Flask, request, jsonify
import requests
from bs4 import BeautifulSoup
import re

@app.route('/get-yopmail', methods=['POST'])
def get_yopmail():
    data = request.json
    email = data.get('email', '')
    mode = data.get('mode', 'household')
    
    shortname = email.split('@')[0]
    if not shortname:
        return jsonify({'success': False, 'message': 'รูปแบบอีเมลไม่ถูกต้อง'})
    
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    try:
        # STEP 1: ดึง yjToken
        wm_res = session.get(
            f'https://yopmail.com/wm?login={shortname}',
            headers=headers, timeout=15
        )
        
        yj_token = ''
        m = re.search(r'var\s+yjToken\s*=\s*["\']([^"\']+)["\']', wm_res.text)
        if m:
            yj_token = m.group(1)
        
        # STEP 2: ดึง inbox (session cookie สำคัญมาก)
        inbox_res = session.get(
            f'https://yopmail.com/en/inbox?login={shortname}&p=1&d=&ctrl=&scrl=&spam=true&yj={yj_token}&v=10.0',
            headers={**headers, 'Referer': f'https://yopmail.com/wm?login={shortname}'},
            timeout=15
        )
        
        # STEP 3: parse IDs
        ids = extract_yopmail_ids(inbox_res.text)
        
        if not ids:
            return jsonify({'success': False, 'message': 'ไม่พบอีเมลใน Yopmail'})
        
        # STEP 4: ดึงเนื้อหาอีเมล
        emails = []
        for i, mid in enumerate(ids[:5]):
            mail_res = session.get(
                f'https://yopmail.com/en/mail?b={shortname}&id={mid}',
                headers={**headers, 'Referer': inbox_res.url},
                timeout=15
            )
            if mail_res.status_code == 200 and mail_res.text:
                soup = BeautifulSoup(mail_res.text, 'html.parser')
                subject = ''
                sub_el = soup.select_one('.ellipsis') or soup.select_one('title')
                if sub_el:
                    subject = sub_el.get_text(strip=True)
                
                mail_div = soup.select_one('#mail') or soup.select_one('.mail')
                html_content = str(mail_div) if mail_div else mail_res.text
                
                emails.append({
                    'id': mid,
                    'subject': subject,
                    'html': html_content,
                    'internalDate': int(__import__('time').time() * 1000) - (i * 60000)
                })
            
            __import__('time').sleep(0.3)
        
        if not emails:
            return jsonify({'success': False, 'message': 'ไม่พบเนื้อหาอีเมลใน Yopmail'})
        
        return jsonify({'success': True, 'emails': emails})
    
    except Exception as e:
        return jsonify({'success': False, 'message': 'ระบบไม่ตอบสนอง กรุณาลองใหม่อีกครั้ง'})


def extract_yopmail_ids(html):
    if not html:
        return []
    ids = []
    seen = set()
    patterns = [
        r'id="([A-Z][a-zA-Z0-9_\-]{3,})"',
        r'href=["\'][^"\']*[?&]id=([^&"\'\\s]{3,})["\']',
        r'readMail\(["\']([^"\']{3,})["\']\)',
    ]
    blacklist = {'mail', 'inbox', 'page', 'login', 'spam', 'ctrl', 'true', 'false'}
    for pattern in patterns:
        for m in re.finditer(pattern, html):
            mid = m.group(1)
            if mid and len(mid) >= 3 and mid not in seen and mid.lower() not in blacklist:
                seen.add(mid)
                ids.append(mid)
        if len(ids) >= 5:
            break
    return ids
